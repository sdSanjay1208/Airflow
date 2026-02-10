import mysql.connector
from datetime import date
from metadata_logger import log_task

def staging_to_curated():
    try:
        conn = mysql.connector.connect(
            host="host.docker.internal",
            user="airflow_user",
            password="airflow_pass",
            database="etl_project"
        )
        cursor = conn.cursor()

        # ✅ Count staging rows
        cursor.execute("SELECT COUNT(*) FROM stg_amazon")
        total_rows = cursor.fetchone()[0]
        print("Total staging rows:", total_rows)

        if total_rows == 0:
            log_task("amazon_etl_pipeline", "staging_to_curated", "SUCCESS", read=0, written=0)
            return

        # ✅ Truncate curated table
        cursor.execute("TRUNCATE TABLE cur_amazon")

        # ✅ Insert with Deduplication + Lookup Standardization
        insert_sql = """
        INSERT INTO cur_amazon (
            order_id, customer_id, product_id, category,
            seller_id, order_date, load_date,
            quantity, unit_price, discount, tax,
            shipping_cost, total_amount,
            payment_method, order_status,
            city, state, country,
            is_current
        )
        SELECT DISTINCT
            s.order_id,
            s.customer_id,
            s.product_id,

            -- ✅ Category Standardization via Lookup
            COALESCE(l.standardized_category, s.category) AS category,

            s.seller_id,
            s.order_date,
            CURRENT_DATE,

            s.quantity,
            s.unit_price,
            s.discount,
            s.tax,
            s.shipping_cost,
            s.total_amount,

            s.payment_method,
            s.order_status,
            s.city,
            s.state,
            s.country,
            1
        FROM stg_amazon s
        LEFT JOIN lkp_category l
            ON UPPER(s.category) = UPPER(l.source_category)
        """

        cursor.execute(insert_sql)

        # ✅ Count curated rows
        cursor.execute("SELECT COUNT(*) FROM cur_amazon")
        written = cursor.fetchone()[0]

        conn.commit()
        cursor.close()
        conn.close()

        log_task(
            "amazon_etl_pipeline",
            "staging_to_curated",
            "SUCCESS",
            read=total_rows,
            written=written
        )

        print("✅ STAGING → CURATED Completed Successfully")

    except Exception as e:
        log_task(
            "amazon_etl_pipeline",
            "staging_to_curated",
            "FAILED",
            error=str(e)
        )
        raise
