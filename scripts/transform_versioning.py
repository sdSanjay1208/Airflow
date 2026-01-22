import hashlib
from metadata_logger import get_conn


def register_transform(transform_name, version, file_path, description):
    with open(file_path, "rb") as f:
        checksum = hashlib.md5(f.read()).hexdigest()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT IGNORE INTO transform_versions
        (transform_name, version, applied_at, checksum, description)
        VALUES (%s, %s, NOW(), %s, %s)
    """, (
        transform_name,
        version,
        checksum,
        description
    ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Transformation registered: {transform_name} {version}")


# ✅ THIS MUST BE OUTSIDE THE FUNCTION
if __name__ == "__main__":

    register_transform(
        transform_name="raw_to_staging_sql",
        version="v1.0",
        file_path="/opt/airflow/data_models/raw_to_stg_transform.sql",
        description="Initial RAW to STAGING SQL transformation"
    )

    register_transform(
        transform_name="staging_to_curated_sql",
        version="v1.0",
        file_path="/opt/airflow/data_models/stg_to_cur_transform.sql",
        description="Initial STAGING to CURATED SQL transformation"
    )
