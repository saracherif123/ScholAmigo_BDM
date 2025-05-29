from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Use paths inside the Airflow container
BASE_DIR = "/opt/airflow"
EXTRACT_SCRIPT = os.path.join(BASE_DIR, "trusted_zone/formatters/Erasmus_Extract_Fields.py")
TRANSFORM_LOAD_SCRIPT = os.path.join(BASE_DIR, "trusted_zone/formatters/Erasmus_clean_load.py")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 5, 28)
}

dag = DAG(
    "erasmus_processing_pipeline",
    default_args=default_args,
    description="Extract, clean, and upload erasmus data to S3",
    schedule_interval="@once",
    catchup=False
)

def run_script(script_path):
    def _run():
        os.system(f"python3 {script_path}")
    return _run

extract_task = PythonOperator(
    task_id="extract_fields",
    python_callable=run_script(EXTRACT_SCRIPT),
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_with_spark",
    python_callable=run_script(TRANSFORM_LOAD_SCRIPT),
    dag=dag,
)


# DAG execution order: extract -> transform -> upload
extract_task >> transform_task