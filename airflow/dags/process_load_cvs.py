from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Use paths inside the Airflow container
BASE_DIR = "/opt/airflow"
EXTRACT_SCRIPT = os.path.join(BASE_DIR, "trusted_zone/formatters/cv_extract_pdfs.py")
TRANSFORM_SCRIPT = os.path.join(BASE_DIR, "trusted_zone/formatters/cv_transformer.py")
UPLOAD_SCRIPT = os.path.join(BASE_DIR, "trusted_zone/formatters/cv_upload_to_s3.py")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 5, 1)
}

dag = DAG(
    "cv_processing_pipeline",
    default_args=default_args,
    description="Extract, transform, and upload CVs to S3",
    schedule_interval="@once",
    catchup=False
)

def run_script(script_path):
    def _run():
        os.system(f"python3 {script_path}")
    return _run

extract_task = PythonOperator(
    task_id="extract_pdfs",
    python_callable=run_script(EXTRACT_SCRIPT),
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_with_spark",
    python_callable=run_script(TRANSFORM_SCRIPT),
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=run_script(UPLOAD_SCRIPT),
    dag=dag,
)

# DAG execution order: extract -> transform -> upload
extract_task >> transform_task >> upload_task