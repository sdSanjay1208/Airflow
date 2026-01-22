from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_hello():
    print("Hello from Airflow, Sanjay!")

default_args = {
    "owner": "sanjay",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="hello_world_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="print_hello",
        python_callable=say_hello,
    )
