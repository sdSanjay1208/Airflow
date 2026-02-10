import mysql.connector

# ✅ Metadata logger
from metadata_logger import log_task


def raw_to_staging():
    try:
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
        # COUNT RAW RECORDS
        # -----------------------------
        cursor.execute("SELECT COUNT(*) FROM raw_amazon")
        records_read = cursor.fetchone()[0]

        # -----------------------------
        # ✅ IMPORTANT FIX
        # CLEAR STAGING BEFORE LOAD
        # -----------------------------
        cursor.execute("TRUNCATE TABLE stg_amazon")

        # -----------------------------
        # INSERT RAW → STAGING
        # -----------------------------
        insert_sql = """
        INSERT INTO stg_amazon (
            order_id, order_date, customer_id, customer_name,
            product_id, product_name, category, brand,
            quantity, unit_price, discount, tax,
            shipping_cost, total_amount, payment_method,
            order_status, city, state, country, seller_id
        )
        SELECT
            order_id,
            order_date,
            customer_id,
            customer_name,
            product_id,
            product_name,
            UPPER(category),      -- ✅ Standardize category
            brand,
            CAST(quantity AS SIGNED),
            CAST(unit_price AS DECIMAL(10,2)),
            CAST(discount AS DECIMAL(10,2)),
            CAST(tax AS DECIMAL(10,2)),
            CAST(shipping_cost AS DECIMAL(10,2)),
            CAST(total_amount AS DECIMAL(12,2)),
            payment_method,
            order_status,
            city,
            state,
            country,
            seller_id
        FROM raw_amazon
        """

        cursor.execute(insert_sql)

        # -----------------------------
        # COUNT STAGING RECORDS
        # -----------------------------
        cursor.execute("SELECT COUNT(*) FROM stg_amazon")
        records_written = cursor.fetchone()[0]

        conn.commit()
        cursor.close()
        conn.close()

        print("✅ RAW → STAGING completed successfully")

        # ✅ Metadata Logging
        log_task(
            "amazon_etl_pipeline",
            "raw_to_staging",
            "SUCCESS",
            read=records_read,
            written=records_written
        )

    except Exception as e:

        log_task(
            "amazon_etl_pipeline",
            "raw_to_staging",
            "FAILED",
            error=str(e)
        )

        raise
