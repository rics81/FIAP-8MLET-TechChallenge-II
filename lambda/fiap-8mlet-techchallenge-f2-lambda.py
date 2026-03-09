import boto3
import logging
import os
from datetime import datetime, timezone, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "fiap-8mlet-techchallenge-f2-glue-etl-refined")
BUCKET_NAME   = os.environ.get("BUCKET_NAME",   "fiap-8mlet-techchallenge-f2-bkt")
RAW_PREFIX    = "b3_daily/raw/"
REFINED_PATH  = f"s3://{BUCKET_NAME}/b3_daily/refined/"

def lambda_handler(event, context):
    glue = boto3.client("glue")
    s3   = boto3.client("s3")

    today = datetime.now(timezone.utc)

    # Find last business day
    weekday = today.weekday()  # 0=Monday, 6=Sunday
    if weekday == 0:           # Monday → go back to Friday
        delta = 3
    elif weekday == 6:         # Sunday → go back to Friday (just in case)
        delta = 2
    else:                      # Tuesday-Saturday → previous day
        delta = 1

    last_business_day = today - timedelta(days=delta)
    today = last_business_day.strftime("%Y%m%d")
    expected = f"{RAW_PREFIX}b3_{today}.parquet"

    logger.info(f"EventBridge trigger fired. Looking for: s3://{BUCKET_NAME}/{expected}")

    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=expected)
        logger.info(f"File found: {expected}")
    except Exception:
        logger.warning(f"File not found: {expected} — scraper may not have run yet. Aborting.")
        return {"statusCode": 200, "body": f"Skipped: {expected} not found in S3."}

    try:
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--s3_input_path":  f"s3://{BUCKET_NAME}/{expected}",
                "--s3_output_path": REFINED_PATH,
                "--execution_date": today,
            }
        )
        job_run_id = response["JobRunId"]
        logger.info(f"Glue job started successfully. JobRunId: {job_run_id}")
        return {"statusCode": 200, "body": f"Glue job started: {job_run_id}"}

    except Exception as e:
        if "ConcurrentRunsExceededException" in str(e):
            logger.warning("Glue job already running — skipping.")
            return {"statusCode": 200, "body": "Skipped: Glue job already running."}
        raise e
