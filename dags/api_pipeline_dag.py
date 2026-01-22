import sys
sys.path.append("/opt/airflow/scripts")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from api_ingestion import run_api_ingestion
from api_clean_transform import clean_api_data
from api_load_to_mysql import load_api_clean_to_mysql

default_args = {
    "owner": "sanjay",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="api_data_pipeline",
    description="External API ingestion and load to MySQL",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # manual trigger for now
    catchup=False,
    tags=["api", "etl"],
) as dag:

    ingest_task = PythonOperator(
        task_id="api_ingestion",
        python_callable=run_api_ingestion
    )

    clean_task = PythonOperator(
        task_id="api_cleaning",
        python_callable=clean_api_data
    )

    load_task = PythonOperator(
        task_id="api_load_mysql",
        python_callable=load_api_clean_to_mysql
    )

    ingest_task >> clean_task >> load_task
