import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Glue job bootstrap ────────────────────────────────────────────────────────
# Resolve the JOB_NAME argument injected by Glue at runtime
args        = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc          = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

print("Glue context ready!")

# ── Path & catalog constants ──────────────────────────────────────────────────
S3_BUCKET         = "fiap-8mlet-techchallenge-f2-bkt"
S3_PREFIX_RAW     = "b3_daily/raw/"
S3_PREFIX_REFINED = "b3_daily/refined/"
GLUE_DATABASE     = "b3_stocks"
GLUE_TABLE        = "b3_refined"
raw_path          = f"s3://{S3_BUCKET}/{S3_PREFIX_RAW}"
refined_path      = f"s3://{S3_BUCKET}/{S3_PREFIX_REFINED}"

print(f"RAW     : {raw_path}")
print(f"REFINED : {refined_path}")
print(f"CATALOG : {GLUE_DATABASE}.{GLUE_TABLE}")

# ── File discovery: pick the 60 most recent raw files ────────────────────────
# 60 days is enough to cover all rolling windows (EMA-20, VWAP-20, 10d high/low)
s3_client = boto3.client("s3")
response  = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX_RAW)

files = sorted(
    [obj for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")],
    key=lambda x: x["Key"],
    reverse=True  # most recent first (files are named b3_YYYYMMDD.parquet)
)

latest_files = files[:60]
latest_paths = [f"s3://{S3_BUCKET}/{f['Key']}" for f in latest_files]

print(f"Total files found : {len(files)}")
print(f"Files selected    : {len(latest_files)}")

# ── Load raw Parquet files into a single Spark DataFrame ─────────────────────
df_raw = spark.read.parquet(*latest_paths)

# Create 'date' column, derive it from the file name using a
# regex: b3_YYYYMMDD.parquet → date column
df_raw = df_raw.withColumn(
    "date",
    F.to_date(
        F.regexp_extract(F.input_file_name(), r"b3_(\d{8})\.parquet", 1),
        "yyyyMMdd",
    ),
)
print("Raw data loaded!")

# ── Column renaming (Req. 5-B) ────────────────────────────────────────────────
# Shorten OHLC column names for convenience throughout the pipeline
df = (
    df_raw
    .withColumnRenamed("high",  "hi")
    .withColumnRenamed("low",   "lo")
    .withColumnRenamed("open",  "op")
    .withColumnRenamed("close", "cl")
)
print("Columns renamed!")

# ── Window definitions ────────────────────────────────────────────────────────
# Base window: per ticker, ordered by date (used for lag/EMA)
w_ticker_date = Window.partitionBy("ticker").orderBy("date")
# 10-row frame: current row + 9 preceding (10 trading days)
w_10          = w_ticker_date.rowsBetween(-9,  0)
# 20-row frame: current row + 19 preceding (20 trading days)
w_20          = w_ticker_date.rowsBetween(-19, 0)

# ── Rolling 10-day high / low (Req. 5-C) ─────────────────────────────────────
df = (
    df
    .withColumn("max_10d", F.max("hi").over(w_10))  # highest intraday high in last 10 sessions
    .withColumn("min_10d", F.min("lo").over(w_10))  # lowest intraday low in last 10 sessions
)
print("Rolling high/low done!")

# ── Exponential Moving Average — EMA-20 (Req. 5-C) ───────────────────────────
# Spark has no native EMA window function, so we approximate it manually:
#   1. Create 20 lag columns (cl_lag0 = today, cl_lag1 = yesterday, …)
#   2. Assign each lag a weight: k × (1 − k)^i  where k = 2 / (N + 1)
#   3. EMA = Σ(price_i × weight_i) / Σ(weight_i)  — normalised to handle NULLs
#      at the start of each ticker's history without producing NaN
N_EMA = 20
k     = 2.0 / (N_EMA + 1)
lag_cols = []
for i in range(N_EMA):
    col_name = f"_lag_{i}"
    df = df.withColumn(col_name, F.lag("cl", i).over(w_ticker_date))
    lag_cols.append((col_name, i))

numerator   = F.lit(0.0)
denominator = F.lit(0.0)
for col_name, i in lag_cols:
    weight      = k * ((1 - k) ** i)
    not_null    = F.col(col_name).isNotNull()
    # Accumulate weighted price; treat NULL lags as 0 contribution
    numerator   = numerator   + F.when(not_null, F.col(col_name) * weight).otherwise(0.0)
    # Accumulate weights only for non-NULL lags (normalisation)
    denominator = denominator + F.when(not_null, F.lit(weight)).otherwise(0.0)

df = df.withColumn("ema_20d", F.round(numerator / denominator, 4))
# Drop all temporary lag columns — they were only needed for the EMA calculation
df = df.drop(*[c for c, _ in lag_cols])
print("EMA 20d done!")

# ── Volume-Weighted Average Price — VWAP-20 (Req. 5-C) ───────────────────────
# VWAP = Σ(close × volume) / Σ(volume) over the trailing 20 trading sessions
df = df.withColumn(
    "vwap_20d",
    F.round(
        F.sum(F.col("cl") * F.col("vol")).over(w_20)
        / F.sum("vol").over(w_20),
        4,
    ),
)
print("VWAP 20d done!")

# ── Build the final refined DataFrame ────────────────────────────────────────
# Select only the columns relevant for downstream analytics; round prices to 4 d.p.
df_refined = df.select(
    "ticker", "date",
    F.round("op", 4).alias("op"),
    F.round("hi", 4).alias("hi"),
    F.round("lo", 4).alias("lo"),
    F.round("cl", 4).alias("cl"),
    "vol",
    F.round("max_10d", 4).alias("max_10d"),
    F.round("min_10d", 4).alias("min_10d"),
    F.round("ema_20d", 4).alias("ema_20d"),
    "vwap_20d"
)
print("Refined dataframe ready!")

# ── Clean refined folder before writing ──────────────────────────────────────
# Full-overwrite strategy: delete all existing refined objects first so stale
# ticker partitions from previous runs do not linger
s3 = boto3.resource("s3")
bucket = s3.Bucket(S3_BUCKET)
deleted = 0
for obj in bucket.objects.filter(Prefix=S3_PREFIX_REFINED):
    obj.delete()
    deleted += 1
print(f"[OK] Cleaned {deleted} objects from refined folder")

# ── Write refined Parquet to S3 (Req. 6) ─────────────────────────────────────
# One file per ticker; Hive-style partitioning makes the data Athena-compatible
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df_refined
    .repartition("ticker")       # one output file per ticker partition
    .write
    .mode("overwrite")
    .partitionBy("ticker")       # s3://.../refined/ticker=PETR4/part-*.parquet
    .parquet(refined_path)
)
print(f"[OK] Refined data written to {refined_path}")

# ── Register in Glue Catalog (Req. 7) ────────────────────────────────────────
glue_client = boto3.client("glue", region_name="sa-east-1")

# Create the database only if it does not exist yet
try:
    glue_client.create_database(DatabaseInput={"Name": GLUE_DATABASE})
    print(f"[OK] Database '{GLUE_DATABASE}' created")
except glue_client.exceptions.AlreadyExistsException:
    print(f"[OK] Database '{GLUE_DATABASE}' already exists")

# Drop and recreate the table so the schema is always in sync with the data
spark.sql(f"DROP TABLE IF EXISTS {GLUE_DATABASE}.{GLUE_TABLE}")

spark.sql(f"""
    CREATE TABLE {GLUE_DATABASE}.{GLUE_TABLE}
    USING PARQUET
    LOCATION '{refined_path}'
""")

# Discover any new ticker partitions written to S3 and add them to the Catalog
spark.sql(f"MSCK REPAIR TABLE {GLUE_DATABASE}.{GLUE_TABLE}")
print(f"[OK] Glue Catalog updated: {GLUE_DATABASE}.{GLUE_TABLE}")

# ── Finalise job ──────────────────────────────────────────────────────────────
job.commit()
print("[OK] Job committed!")
