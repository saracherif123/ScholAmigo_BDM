import boto3
import os
import json
from botocore.exceptions import ClientError

# S3 configuration
S3_BUCKET = "scholamigo"
S3_PREFIX = "trusted_zone_data/resume_data/"

# Use paths inside the Airflow container
LOCAL_FOLDER = "/opt/airflow/trusted_zone/data/resume_data/cleaned/"

def upload_folder_to_s3():
    s3_client = boto3.client('s3')
    
    # List all JSON files in the local folder
    json_files = [f for f in os.listdir(LOCAL_FOLDER) if f.endswith('.json')]
    
    for json_file in json_files:
        local_path = os.path.join(LOCAL_FOLDER, json_file)
        s3_key = f"{S3_PREFIX}{json_file}"
        
        try:
            # Upload the file
            s3_client.upload_file(local_path, S3_BUCKET, s3_key)
            print(f"Successfully uploaded {json_file} to s3://{S3_BUCKET}/{s3_key}")
        except ClientError as e:
            print(f"Error uploading {json_file}: {str(e)}")

if __name__ == "__main__":
    upload_folder_to_s3()
