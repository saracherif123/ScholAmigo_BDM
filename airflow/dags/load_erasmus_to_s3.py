from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import boto3
import json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'load_erasmus_to_s3',
    default_args=default_args,
    description='Load Erasmus scholarship data to S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
)

def process_erasmus_data(**context):
    # Initialize AWS S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )

    # S3 configuration
    bucket_name = 'scholamigo'
    prefix = 'landing_zone_data/erasmus_data/' #should be changed to where the updated files are for trusted zone
    
    # List all objects in S3
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    all_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]
    
    print(f"Found {len(all_files)} JSON files to process")
    
    # Process each file
    for file_key in all_files:
        try:
            # Read the file from S3
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            
            # Upload to trusted zone with timestamp
            output_key = f"trusted_zone_data/erasmus_data/processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(file_key)}"
            s3.put_object(
                Bucket=bucket_name,
                Key=output_key,
                Body=json.dumps(data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            print(f" Successfully processed {file_key}")
            print(f" Output saved to {output_key}")
        except Exception as e:
            print(f" Error processing {file_key}: {e}")

# Define the task
process_task = PythonOperator(
    task_id='process_erasmus_data',
    python_callable=process_erasmus_data,
    dag=dag,
)

# Set task dependencies
process_task 