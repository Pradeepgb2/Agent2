import os
import boto3

s3 = boto3.client("s3")

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

def upload_file(local_path: str, s3_key: str):
    if not BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME is not configured")

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File not found: {local_path}")

    s3.upload_file(local_path, BUCKET_NAME, s3_key)

    print(f"[S3] Uploaded {local_path} → s3://{BUCKET_NAME}/{s3_key}")
