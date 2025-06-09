from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import boto3

# Constants
COLLECTORS = [
    "erasmus_collector.py",
    "resume_collector.py",
    "uk_collector.py",
    "LinkedinProfiles_collector.py",
    "scholarship_collector.py",
    "users_collector.py",
]

SCRIPT_DIR = "/opt/airflow/landing_zone/collectors"
DATA_DIR = "/opt/airflow/landing_zone/data"
S3_BUCKET = "scholamigo"
S3_PREFIX = "landing_zone_data/" 

# Task to run a specific collector
def run_collector(script_name):
    def _run():
        script_path = os.path.join(SCRIPT_DIR, script_name)
        print(f"🚀 Running: {script_path}")
        os.system(f"python3 {script_path}")
    return _run

def upload_to_s3():
    s3 = boto3.client("s3")
    for root, _, files in os.walk(DATA_DIR):
        for file in files:
            local_path = os.path.join(root, file)
            rel_path = os.path.relpath(local_path, DATA_DIR)
            s3_key = f"{S3_PREFIX}{rel_path}"

            # Extract a source name from the path (e.g., resume_data)
            source = rel_path.split(os.sep)[0]

            # Upload with metadata
            s3.upload_file(
                local_path,
                S3_BUCKET,
                s3_key,
                ExtraArgs={
                    'Metadata': {
                        'source': source,
                        'upload-timestamp': datetime.utcnow().isoformat()
                    }
                }
            )
            print(f"✅ Uploaded with metadata: {s3_key}")

# DAG definition
with DAG(
    dag_id="scholamigo_dag",
    start_date=datetime(2025, 4, 5),
    schedule_interval="@once",  # change if you want scheduled runs
    catchup=False,
) as dag:

    previous = None
    for script in COLLECTORS:
        task = PythonOperator(
            task_id=f"run_{script.replace('.py', '')}",
            python_callable=run_collector(script)
        )
        if previous:
            previous >> task
        previous = task

    upload_task = PythonOperator(
        task_id="upload_all_to_s3",
        python_callable=upload_to_s3
    )

    previous >> upload_task