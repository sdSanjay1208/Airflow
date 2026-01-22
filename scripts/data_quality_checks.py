import mysql.connector

def data_quality_checks():

    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="airflow_user",
        password="airflow_pass",
        database="etl_project"
    )

    cursor = conn.cursor()

    # 1️⃣ Row count comparison
    cursor.execute("SELECT COUNT(*) FROM stg_amazon")
    staging_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM cur_amazon")
    curated_count = cursor.fetchone()[0]

    if curated_count == 0:
        raise ValueError("❌ Curated table is EMPTY – transformation failed")

    if curated_count != staging_count:
        raise ValueError(f"""
        ❌ Row count mismatch
        staging={staging_count}
        curated={curated_count}
        """)

    # 2️⃣ Duplicate check
    cursor.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT order_id, product_id, COUNT(*)
            FROM cur_amazon
            GROUP BY order_id, product_id
            HAVING COUNT(*) > 1
        ) dup
    """)
    dup_count = cursor.fetchone()[0]

    if dup_count > 0:
        raise ValueError(f"❌ Duplicate rows exist in curated table: {dup_count}")

    # 3️⃣ Null key check
    cursor.execute("""
        SELECT COUNT(*)
        FROM cur_amazon
        WHERE order_id IS NULL
    """)
    null_orders = cursor.fetchone()[0]

    if null_orders > 0:
        raise ValueError(f"❌ NULL order_id detected: {null_orders}")

    # 4️⃣ Category not mapped in dimension table
    cursor.execute("""
        SELECT COUNT(*)
        FROM cur_amazon f
        LEFT JOIN dim_category d
          ON f.category = d.category_name
        WHERE d.category_name IS NULL
    """)
    unmapped = cursor.fetchone()[0]

    if unmapped > 0:
        raise ValueError(f"❌ Unmapped category values: {unmapped}")

    conn.close()

    print("✅ DATA QUALITY PASSED")
