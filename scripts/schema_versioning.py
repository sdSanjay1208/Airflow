import hashlib
from metadata_logger import get_conn

def register_schema(schema_name, version, sql_file_path, description):
    with open(sql_file_path, "rb") as f:
        checksum = hashlib.md5(f.read()).hexdigest()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT IGNORE INTO schema_versions
        (schema_name, version, applied_at, checksum, description)
        VALUES (%s, %s, NOW(), %s, %s)
    """, (
        schema_name,
        version,
        checksum,
        description
    ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Schema registered: {schema_name} {version}")


# -------------------------------
# AUTO-RUN REGISTRATION
# -------------------------------
if __name__ == "__main__":

    register_schema(
        schema_name="cur_amazon",
        version="v1.0",
        sql_file_path="/opt/airflow/data_models/schemas/cur_amazon_v1.sql",
        description="Initial curated schema"
    )

    register_schema(
        schema_name="dim_category",
        version="v1.0",
        sql_file_path="/opt/airflow/data_models/schemas/dim_category_v1.sql",
        description="Initial category dimension schema"
    )

    register_schema(
        schema_name="api_products",
        version="v1.0",
        sql_file_path="/opt/airflow/data_models/schemas/api_products_v1.sql",
        description="Initial API products schema"
    )
