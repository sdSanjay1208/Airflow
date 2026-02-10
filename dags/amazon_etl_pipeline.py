import sys
import os

# ✅ Add scripts folder to Airflow path
sys.path.append("/opt/airflow/scripts")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# ✅ ETL Tasks
from csv_to_raw import csv_to_raw
from raw_to_staging import raw_to_staging
from staging_to_curated import staging_to_curated
from validate_pipeline import validate_pipeline
from scd_category import scd_category_load

# ✅ Metadata Logger
from metadata_logger import log_dag_run


# -------------------------------------------------
# ✅ DAG CALLBACKS (LOG DAG SUCCESS / FAILURE)
# -------------------------------------------------
def dag_success_callback(context):
    """
    Runs automatically when the full DAG succeeds
    Logs DAG run status into etl_dag_run_log table
    """
    log_dag_run("amazon_etl_pipeline", "SUCCESS")


def dag_failure_callback(context):
    """
    Runs automatically when DAG fails
    Logs failure + exception into metadata table
    """
    error = str(context.get("exception"))
    log_dag_run("amazon_etl_pipeline", "FAILED", error=error)


# -------------------------------------------------
# ✅ DEFAULT ARGUMENTS
# -------------------------------------------------
default_args = {
    "owner": "sanjay",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# -------------------------------------------------
# ✅ MAIN DAG DEFINITION (TASK 8 STEP 2 OPTIMIZED)
# -------------------------------------------------
with DAG(
    dag_id="amazon_etl_pipeline",
    description="Amazon Enterprise ETL Pipeline with SCD Type 2 (RAW → STAGING → CURATED → DIM)",

    default_args=default_args,

    # ✅ Scheduling Optimization (Task 8 – Step 2)
    start_date=datetime(2025, 1, 1),

    schedule_interval=None,   # ✅ Manual Trigger (best for project/demo)

    catchup=False,            # ✅ No backfill runs

    max_active_runs=1,        # ✅ Prevent overlapping DAG executions

    tags=["amazon", "etl", "scd2", "enterprise"],

    # ✅ Metadata DAG Logging Hooks
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback

) as dag:

    # -------------------------------------------------
    # ✅ TASK 1: CSV → RAW
    # -------------------------------------------------
    csv_to_raw_task = PythonOperator(
        task_id="csv_to_raw",
        python_callable=csv_to_raw
    )

    # -------------------------------------------------
    # ✅ TASK 2: RAW → STAGING
    # -------------------------------------------------
    raw_to_staging_task = PythonOperator(
        task_id="raw_to_staging",
        python_callable=raw_to_staging
    )

    # -------------------------------------------------
    # ✅ TASK 3: STAGING → CURATED
    # -------------------------------------------------
    staging_to_curated_task = PythonOperator(
        task_id="staging_to_curated",
        python_callable=staging_to_curated
    )

    # -------------------------------------------------
    # ✅ TASK 4: CURATED → DIM CATEGORY (SCD TYPE 2)
    # -------------------------------------------------
    scd_category_task = PythonOperator(
        task_id="scd_category_load",
        python_callable=scd_category_load
    )

    # -------------------------------------------------
    # ✅ TASK 5: VALIDATION CHECKS
    # -------------------------------------------------
    validate_task = PythonOperator(
        task_id="validate_pipeline",
        python_callable=validate_pipeline
    )

    # -------------------------------------------------
    # ✅ FINAL DAG FLOW
    # -------------------------------------------------
    csv_to_raw_task >> raw_to_staging_task >> staging_to_curated_task >> scd_category_task >> validate_task
