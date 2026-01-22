import mysql.connector
from datetime import date
from fuzzy_mapper import fuzzy_match
from metadata_logger import log_task


def staging_to_curated():
    try:
        # -----------------------------
        # CONFIG
        # -----------------------------
        BATCH_SIZE = 500   # Safe chunk size for MySQL

        # -----------------------------
        # CONNECT TO MYSQL
        # -----------------------------
        conn = mysql.connector.connect(
            host="host.docker.internal",
            user="airflow_user",
            password="airflow_pass",
            database="etl_project"
        )
        cursor = conn.cursor(dictionary=True)

        # -----------------------------
        # FETCH STAGING DATA
        # -----------------------------
        cursor.execute("SELECT * FROM stg_amazon")
        rows = cursor.fetchall()

        if not rows:
            print("No data found in staging table")
            log_task("amazon_etl_pipeline", "staging_to_curated", "SUCCESS", read=0, written=0)
            return

        records_read = len(rows)

        # -----------------------------
        # FULL LOAD (TRUNCATE)
        # -----------------------------
        cursor.execute("TRUNCATE TABLE cur_amazon")

        # -----------------------------
        # LOAD LOOKUP TABLE
        # -----------------------------
        cursor.execute("""
            SELECT source_category, standardized_category
            FROM lkp_category
            WHERE is_active = TRUE
        """)

        category_lookup = {
            row["source_category"].upper(): row["standardized_category"]
            for row in cursor.fetchall()
        }

        # Used only for fuzzy fallback
        standard_categories = list(set(category_lookup.values()))

        # -----------------------------
        # INSERT STATEMENT
        # -----------------------------
        insert_sql = """
            INSERT INTO cur_amazon (
                order_id,
                customer_id,
                product_id,
                category,
                seller_id,
                order_date,
                load_date,
                quantity,
                unit_price,
                discount,
                tax,
                shipping_cost,
                total_amount,
                payment_method,
                order_status,
                city,
                state,
                country,
                is_current
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # -----------------------------
        # TRANSFORM + COLLECT
        # -----------------------------
        batch_data = []

        for row in rows:
            raw_category = row["category"]
            raw_upper = raw_category.upper() if raw_category else None

            # 1️⃣ Lookup
            if raw_upper in category_lookup:
                final_category = category_lookup[raw_upper]
            else:
                # 2️⃣ Fuzzy fallback
                matched = fuzzy_match(raw_upper, standard_categories)
                final_category = matched if matched else raw_category

            batch_data.append((
                row["order_id"],
                row["customer_id"],
                row["product_id"],
                final_category.title(),
                row["seller_id"],
                row["order_date"],
                date.today(),
                row["quantity"],
                row["unit_price"],
                row["discount"],
                row["tax"],
                row["shipping_cost"],
                row["total_amount"],
                row["payment_method"],
                row["order_status"],
                row["city"],
                row["state"],
                row["country"],
                True
            ))

        # -----------------------------
        # CHUNKED INSERT
        # -----------------------------
        for i in range(0, len(batch_data), BATCH_SIZE):
            chunk = batch_data[i:i + BATCH_SIZE]
            cursor.executemany(insert_sql, chunk)
            conn.commit()

        cursor.close()
        conn.close()

        # -----------------------------
        # METADATA LOGGING
        # -----------------------------
        log_task(
            "amazon_etl_pipeline",
            "staging_to_curated",
            "SUCCESS",
            read=records_read,
            written=len(batch_data)
        )

        print(f"STAGING → CURATED completed successfully ({len(batch_data)} rows loaded)")

    except Exception as e:
        log_task(
            "amazon_etl_pipeline",
            "staging_to_curated",
            "FAILED",
            error=str(e)
        )
        raise
