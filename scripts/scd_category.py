import mysql.connector
from datetime import date


def scd_category_load():
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="airflow_user",
        password="airflow_pass",
        database="etl_project"
    )
    cursor = conn.cursor(dictionary=True)

    # 1️⃣ Get distinct categories from curated table
    cursor.execute("SELECT DISTINCT category FROM cur_amazon")
    categories = cursor.fetchall()

    for row in categories:
        category = row["category"]

        # 2️⃣ Check if category already exists as active
        cursor.execute("""
            SELECT category_sk
            FROM dim_category
            WHERE category_name = %s
              AND is_current = TRUE
        """, (category,))
        existing = cursor.fetchone()

        if existing:
            # Category already active → do nothing
            continue

        # 3️⃣ Expire old records (same name but inactive)
        cursor.execute("""
            UPDATE dim_category
            SET end_date = %s,
                is_current = FALSE
            WHERE category_name = %s
              AND is_current = TRUE
        """, (date.today(), category))

        # 4️⃣ Insert new version
        cursor.execute("""
            INSERT INTO dim_category (
                category_name,
                start_date,
                end_date,
                is_current
            )
            VALUES (%s, %s, NULL, TRUE)
        """, (category, date.today()))

    conn.commit()
    cursor.close()
    conn.close()

    print("SCD Type 2 Category Load Completed")
