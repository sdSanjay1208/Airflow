import pandas as pd
import mysql.connector
from metadata_logger import log_task


def csv_to_raw():
    try:
        csv_path = "/opt/airflow/data/raw/Amazon.csv"
        df = pd.read_csv(csv_path)

        rows_loaded = len(df)

        conn = mysql.connector.connect(
            host="host.docker.internal",
            user="airflow_user",
            password="airflow_pass",
            database="etl_project"
        )
        cursor = conn.cursor()

        # ✅ FIX 1: Clear RAW before reloading
        cursor.execute("TRUNCATE TABLE raw_amazon")

        insert_sql = """
            INSERT INTO raw_amazon (
                order_id, order_date, customer_id, customer_name,
                product_id, product_name, category, brand,
                quantity, unit_price, discount, tax,
                shipping_cost, total_amount, payment_method,
                order_status, city, state, country, seller_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_sql, df.values.tolist())
        conn.commit()

        cursor.close()
        conn.close()

        print("✅ CSV successfully loaded into RAW layer")

        log_task(
            "amazon_etl_pipeline",
            "csv_to_raw",
            "SUCCESS",
            written=rows_loaded
        )

    except Exception as e:

        log_task(
            "amazon_etl_pipeline",
            "csv_to_raw",
            "FAILED",
            error=str(e)
        )
        raise
