import mysql.connector

def get_latest_pipeline_status():
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="airflow_user",
        password="airflow_pass",
        database="etl_project"
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM vw_latest_pipeline_status")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result


def get_failed_tasks():
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="airflow_user",
        password="airflow_pass",
        database="etl_project"
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM vw_failed_tasks")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows
