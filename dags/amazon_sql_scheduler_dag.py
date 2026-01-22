import sys
sys.path.append("/opt/airflow/scripts")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from run_sql_file import run_sql_file
from data_quality_checks import data_quality_checks

default_args = {
    "owner": "sanjay",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="amazon_sql_transformation_scheduler",
    description="Schedule SQL-based transformations (RAW → STG → CUR)",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # manual trigger
    catchup=False,
    tags=["sql", "etl", "scheduler"],
) as dag:

    raw_to_stg = PythonOperator(
        task_id="raw_to_stg_sql",
        python_callable=run_sql_file,
        op_args=["/opt/airflow/data_models/raw_to_stg_transform.sql"],
    )

    stg_to_cur = PythonOperator(
        task_id="stg_to_cur_sql",
        python_callable=run_sql_file,
        op_args=["/opt/airflow/data_models/stg_to_cur_transform.sql"],
    )

    validate = PythonOperator(
        task_id="validate_sql",
        python_callable=run_sql_file,
        op_args=["/opt/airflow/data_models/validation_checks.sql"],
    )

    dq_check_task = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks
    )

    raw_to_stg >> stg_to_cur >> validate >> dq_check_task
