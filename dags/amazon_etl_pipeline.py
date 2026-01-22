import sys
import os
sys.path.append("/opt/airflow/scripts")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# ETL tasks
from csv_to_raw import csv_to_raw
from raw_to_staging import raw_to_staging
from staging_to_curated import staging_to_curated
from validate_pipeline import validate_pipeline
from scd_category import scd_category_load

# 🟢 Metadata logger
from metadata_logger import log_dag_run


# -----------------------------
# DAG CALLBACKS
# -----------------------------
def dag_success_callback(context):
    log_dag_run("amazon_etl_pipeline", "SUCCESS")


def dag_failure_callback(context):
    error = str(context.get("exception"))
    log_dag_run("amazon_etl_pipeline", "FAILED", error=error)


default_args = {
    "owner": "sanjay",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="amazon_etl_pipeline",
    description="Amazon CSV ETL Pipeline with SCD Type 2 (RAW → STAGING → CURATED → DIM)",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["amazon", "etl", "scd2"],

    # 🟢 DAG-level audit hooks
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback
) as dag:

    csv_to_raw_task = PythonOperator(
        task_id="csv_to_raw",
        python_callable=csv_to_raw
    )

    raw_to_staging_task = PythonOperator(
        task_id="raw_to_staging",
        python_callable=raw_to_staging
    )

    staging_to_curated_task = PythonOperator(
        task_id="staging_to_curated",
        python_callable=staging_to_curated
    )

    scd_category_task = PythonOperator(
        task_id="scd_category_load",
        python_callable=scd_category_load
    )

    validate_task = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline
    )

    csv_to_raw_task >> raw_to_staging_task >> staging_to_curated_task >> scd_category_task >> validate_task
