import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging
import os
import re
from datetime import datetime, date

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_latest_date_from_s3(bucket, s3_prefix=''):
    """
    Scan S3 for existing b3_yyyymmdd.parquet files and return the most recent date found.
    Returns a datetime.date object, or None if no files are found.
    """
    s3_client = boto3.client('s3')
    pattern = re.compile(r'b3_(\d{8})\.parquet$')
    dates_found = []

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=s3_prefix)

    try:
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                match = pattern.search(key)
                if match:
                    date_str = match.group(1)
                    try:
                        parsed = datetime.strptime(date_str, '%Y%m%d').date()
                        dates_found.append(parsed)
                    except ValueError:
                        continue
    except ClientError as e:
        logging.error(f"Failed to list objects in s3://{bucket}/{s3_prefix}: {e}")
        return None

    if not dates_found:
        logging.info("No existing b3_*.parquet files found in S3.")
        return None

    latest = max(dates_found)
    logging.info(f"Latest date found in S3: {latest}")
    return latest


def upload_to_s3(file_paths, bucket, s3_prefix=''):
    """
    Upload a list of files to an S3 bucket.
    Returns list of S3 object keys that were successfully uploaded.
    """
    s3_client = boto3.client('s3')
    uploaded_keys = []
    for local_path in file_paths:
        if not os.path.exists(local_path):
            logging.warning(f"File not found: {local_path}")
            continue
        file_name = os.path.basename(local_path)
        s3_key = s3_prefix + file_name if s3_prefix else file_name
        try:
            s3_client.upload_file(local_path, bucket, s3_key)
            logging.info(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")
            uploaded_keys.append(s3_key)
        except (NoCredentialsError, ClientError) as e:
            logging.error(f"Failed to upload {local_path}: {e}")
    return uploaded_keys


if __name__ == "__main__":
    # Example usage
    latest = get_latest_date_from_s3('my-bucket', s3_prefix='daily/')
    print(f"Latest date in S3: {latest}")

    files = ['b3_20250221.parquet', 'b3_20250222.parquet']
    upload_to_s3(files, bucket='my-bucket', s3_prefix='daily/')