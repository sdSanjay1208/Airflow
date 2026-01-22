import mysql.connector
from datetime import datetime
import hashlib
import os


# -----------------------------
# DB CONNECTION
# -----------------------------
def get_conn():
    return mysql.connector.connect(
        host="host.docker.internal",
        user="airflow_user",
        password="airflow_pass",
        database="etl_project"
    )


# -----------------------------
# DAG LEVEL LOGGING
# -----------------------------
def log_dag_run(dag_id, status, records=None, error=None):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO etl_dag_run_log
        (dag_id, start_time, end_time, status, records_processed, error_message)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        dag_id,
        datetime.now(),
        datetime.now(),
        status,
        records,
        error
    ))

    conn.commit()
    cur.close()
    conn.close()


# -----------------------------
# TASK LEVEL LOGGING
# -----------------------------
def log_task(dag_id, task_id, status, read=None, written=None, error=None):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO etl_task_log
        (dag_id, task_id, start_time, end_time, status,
         records_read, records_written, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        dag_id,
        task_id,
        datetime.now(),
        datetime.now(),
        status,
        read,
        written,
        error
    ))

    conn.commit()
    cur.close()
    conn.close()


# -----------------------------
# 🔴 SCRIPT VERSION REGISTRY
# -----------------------------
def register_script(script_name, version, file_path):
    """
    Registers ETL script version with checksum
    """

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Script not found: {file_path}")

    with open(file_path, "rb") as f:
        checksum = hashlib.md5(f.read()).hexdigest()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT IGNORE INTO script_versions
        (script_name, version, registered_at, checksum)
        VALUES (%s, %s, NOW(), %s)
    """, (
        script_name,
        version,
        checksum
    ))

    conn.commit()
    cur.close()
    conn.close()
