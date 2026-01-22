import pandas as pd
import mysql.connector
import hashlib

# 🔴 Metadata logging
from metadata_logger import log_task


def csv_to_raw():
    try:
        csv_path = "/opt/airflow/data/raw/Amazon.csv"

        # -----------------------------
        # READ CSV
        # -----------------------------
        df = pd.read_csv(csv_path)

        rows_loaded = len(df)

        # 🔴 DATASET CHECKSUM (VERSIONING)
        data_str = df.to_csv(index=False)
        checksum = hashlib.md5(data_str.encode()).hexdigest()

        # -----------------------------
        # CONNECT TO MYSQL
        # -----------------------------
        conn = mysql.connector.connect(
            host="host.docker.internal",
            user="airflow_user",
            password="airflow_pass",
            database="etl_project"
        )
        cursor = conn.cursor()

        # -----------------------------
        # INSERT INTO RAW TABLE
        # -----------------------------
        insert_sql = """
            INSERT INTO raw_amazon (
                order_id, order_date, customer_id, customer_name,
                product_id, product_name, category, brand,
                quantity, unit_price, discount, tax,
                shipping_cost, total_amount, payment_method,
                order_status, city, state, country, seller_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            cursor.execute(insert_sql, tuple(row))

        # -----------------------------
        # COMMIT RAW LOAD
        # -----------------------------
        conn.commit()

        # -----------------------------
        # 🔴 DATASET VERSION LOG
        # -----------------------------
        cursor.execute("""
            INSERT INTO dataset_versions (
                dataset_name,
                load_time,
                record_count,
                checksum,
                status
            )
            VALUES (%s, NOW(), %s, %s, 'LOADED')
        """, ("Amazon.csv", rows_loaded, checksum))

        conn.commit()

        cursor.close()
        conn.close()

        print("✅ CSV successfully loaded into RAW layer")
        print(f"📦 Dataset checksum: {checksum}")

        # -----------------------------
        # 🔴 TASK METADATA LOG (SUCCESS)
        # -----------------------------
        log_task(
            "amazon_etl_pipeline",
            "csv_to_raw",
            "SUCCESS",
            written=rows_loaded
        )

    except Exception as e:

        # -----------------------------
        # 🔴 TASK METADATA LOG (FAILURE)
        # -----------------------------
        log_task(
            "amazon_etl_pipeline",
            "csv_to_raw",
            "FAILED",
            error=str(e)
        )

        raise
