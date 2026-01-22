import mysql.connector

def run_sql_file(file_path):

    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="airflow_user",
        password="airflow_pass",
        database="etl_project"
    )

    # IMPORTANT – buffered cursor avoids unread result error
    cursor = conn.cursor(buffered=True)

    with open(file_path, "r") as f:
        sql = f.read()

    # Split statements by semicolon
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    for stmt in statements:
        print(f"Running SQL:\n{stmt}\n")
        cursor.execute(stmt)

        # If SELECT – fetch results so connection clears result buffer
        if stmt.lower().startswith("select"):
            cursor.fetchall()

    conn.commit()
    cursor.close()
    conn.close()

    print("SQL file executed successfully")
