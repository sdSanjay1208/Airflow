import mysql.connector
import pandas as pd
import math


def load_api_clean_to_mysql():

    # 1) read cleaned CSV
    df = pd.read_csv("/opt/airflow/data/staging/api_clean.csv")

    # 2) handle missing values safely
    df = df.fillna("")

    # 3) connect DB
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="airflow_user",
        password="airflow_pass",
        database="etl_project"
    )

    cursor = conn.cursor()

    insert_sql = """
        REPLACE INTO api_products_raw (
            id,
            title,
            category,
            price,
            description,
            image_url
        )
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    rows_loaded = 0

    for _, row in df.iterrows():

        # convert price safely
        try:
            price = float(row["price"])
        except:
            price = 0.0

        cursor.execute(insert_sql, (
            int(row["id"]),
            row["title"],
            row["category"],
            price,
            row.get("description", ""),
            row.get("image", row.get("image_url", ""))
        ))

        rows_loaded += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {rows_loaded} API records into MySQL")


# 🔹 ensures script runs when called from docker exec
if __name__ == "__main__":
    load_api_clean_to_mysql()
