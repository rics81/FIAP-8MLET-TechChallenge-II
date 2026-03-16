import boto3
import logging
import os
from datetime import datetime, timezone, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "fiap-8mlet-techchallenge-f2-glue-etl-refined")
BUCKET_NAME   = os.environ.get("BUCKET_NAME",   "fiap-8mlet-techchallenge-f2-bkt")
RAW_PREFIX    = "b3_daily/raw/"


def last_business_day_str():
    today = datetime.now(timezone.utc)
    if today.weekday() == 0:    # Monday
        delta = 3
    else:
        if today.weekday() == 6: # Sunday
            delta = 2
        else:                   # Any other day
            delta = 1
    return(today - timedelta(days=delta)).strftime("%Y%m%d")

def file_exists(s3, bucket, key):
    result = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return result.get("KeyCount", 0) > 0


def lambda_handler(event, context):
    s3   = boto3.client("s3")
    glue = boto3.client("glue")

    date_str = last_business_day_str()
    expected = f"{RAW_PREFIX}b3_{date_str}.parquet"
    logger.info(f"Looking for: s3://{BUCKET_NAME}/{expected}")

    if not file_exists(s3, BUCKET_NAME, expected):
        logger.warning(f"File not found: {expected} — scraper may not have run yet. Aborting.")
        return {"statusCode": 200, "body": f"Skipped: {expected} not found in S3."}

    try:
        response = glue.start_job_run(JobName=GLUE_JOB_NAME)
        job_run_id = response["JobRunId"]
        logger.info(f"Glue job started. JobRunId: {job_run_id}")
        return {"statusCode": 200, "body": f"Glue job started: {job_run_id}"}
    except glue.exceptions.ConcurrentRunsExceededException:
        logger.warning("Glue job already running — skipping.")
        return {"statusCode": 200, "body": "Skipped: Glue job already running."}
