import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    files = ['b3_20250221.parquet', 'b3_20250222.parquet']
    upload_to_s3(files, bucket='my-bucket', s3_prefix='daily/')