import mysql.connector


# ✅ Reusable Connection Function
def get_conn():
    return mysql.connector.connect(
        host="host.docker.internal",
        user="airflow_user",
        password="airflow_pass",
        database="etl_project"
    )


# ✅ Latest Pipeline Status
def get_latest_pipeline_status():
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM vw_latest_pipeline_status")
    result = cursor.fetchone()

    cursor.close()
    conn.close()
    return result


# ✅ Failed Task List
def get_failed_tasks():
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM vw_failed_tasks")
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows


# ✅ Pipeline History API (Task 7 Requirement)
def get_pipeline_history():
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT *
        FROM pipeline_history
        ORDER BY run_date DESC
        LIMIT 30
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows


# ✅ Data Quality / Volume Trend API
def get_dq_scores():
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT
            run_date,
            total_records,
            status
        FROM pipeline_history
        ORDER BY run_date
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows
