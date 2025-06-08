from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Use paths inside the Airflow container
BASE_DIR = "/opt/airflow"
CLEANER_SCRIPT = os.path.join(BASE_DIR, "trusted_zone/formatters/student_alumni_cleaner.py")
GRAPH_CREATION_SCRIPT = os.path.join(BASE_DIR, "exploitation_zone/formatters/student_alumni_graph_creation.py")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 5, 28)
}

dag = DAG(
    "student_alumni_processing_pipeline",
    default_args=default_args,
    description="Clean student alumni data and create graphs",
    schedule_interval="@once",
    catchup=False
)

def run_script(script_path):
    def _run():
        os.system(f"python3 {script_path}")
    return _run

clean_task = PythonOperator(
    task_id="clean_student_alumni_data",
    python_callable=run_script(CLEANER_SCRIPT),
    dag=dag,
)

graph_creation_task = PythonOperator(
    task_id="create_student_alumni_graphs",
    python_callable=run_script(GRAPH_CREATION_SCRIPT),
    dag=dag,
)

# DAG execution order: clean -> create graphs
clean_task >> graph_creation_task