import boto3
import os

S3_BUCKET = "scholamigo"
S3_PREFIX = "trusted_zone_data/resume_data/"

# Local path inside the container where transformed data is saved
LOCAL_FOLDER = "/opt/airflow/trusted_zone/data/resume_data/cleaned/"

def upload_folder_to_s3(local_folder, s3_bucket, s3_prefix):
    s3 = boto3.client("s3")
    for root, _, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_folder)
            s3_key = os.path.join(s3_prefix, relative_path)
            s3.upload_file(local_path, s3_bucket, s3_key)
            print(f" Uploaded: s3://{s3_bucket}/{s3_key}")

if __name__ == "__main__":
    upload_folder_to_s3(LOCAL_FOLDER, S3_BUCKET, S3_PREFIX)
