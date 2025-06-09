from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Base directory inside the Airflow container
BASE_DIR = "/opt/airflow"
TRUSTED_ZONE = os.path.join(BASE_DIR, "trusted_zone", "formatters")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 5, 28)
}

dag = DAG(
    "scholarship_processing_pipeline",
    default_args=default_args,
    description="Extract, clean, and upload multiple scholarship datasets to S3",
    schedule_interval="@once",
    catchup=False
)

def run_script(script_path):
    def _run():
        os.system(f"python3 {script_path}")
    return _run

# Paths to scripts
SCRIPT_PATHS = {
    "erasmus_extract": os.path.join(TRUSTED_ZONE, "Erasmus_Extract_Fields.py"),
    "erasmus_clean": os.path.join(TRUSTED_ZONE, "Erasmus_clean_load.py"),
    "uk_extract": os.path.join(TRUSTED_ZONE, "UK_Scholarships_Extract_Fields.py"),
    "uk_clean": os.path.join(TRUSTED_ZONE, "UK_Scholarships_clean_load.py"),
    "aggregator": os.path.join(TRUSTED_ZONE, "aggregator_processor.py"),
}

# Define tasks
erasmus_extract_task = PythonOperator(
    task_id="extract_erasmus_fields",
    python_callable=run_script(SCRIPT_PATHS["erasmus_extract"]),
    dag=dag,
)

erasmus_clean_task = PythonOperator(
    task_id="clean_erasmus_data",
    python_callable=run_script(SCRIPT_PATHS["erasmus_clean"]),
    dag=dag,
)

uk_extract_task = PythonOperator(
    task_id="extract_uk_fields",
    python_callable=run_script(SCRIPT_PATHS["uk_extract"]),
    dag=dag,
)

uk_clean_task = PythonOperator(
    task_id="clean_uk_data",
    python_callable=run_script(SCRIPT_PATHS["uk_clean"]),
    dag=dag,
)

aggregator_task = PythonOperator(
    task_id="run_aggregator",
    python_callable=run_script(SCRIPT_PATHS["aggregator"]),
    dag=dag,
)

# Task dependencies
erasmus_extract_task >> erasmus_clean_task
uk_extract_task >> uk_clean_task
[erasmus_clean_task, uk_clean_task] >> aggregator_task
