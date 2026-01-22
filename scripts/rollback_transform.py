import mysql.connector
from metadata_logger import get_conn


def rollback_transform(transform_name, version):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    # 1️⃣ Get SQL file path from transform_versions
    cur.execute("""
        SELECT checksum
        FROM transform_versions
        WHERE transform_name = %s AND version = %s
    """, (transform_name, version))

    row = cur.fetchone()

    if not row:
        raise Exception("❌ Transformation version not found")

    print(f"Rolling back {transform_name} to version {version}")

    # 2️⃣ Decide rollback action (example: truncate + re-run SQL)
    if transform_name == "staging_to_curated_sql":
        cur.execute("TRUNCATE TABLE cur_amazon")
        conn.commit()
        print("Curated table truncated")

    cur.close()
    conn.close()

    print("✅ Rollback completed")
    if __name__ == "__main__":
        rollback_transform("staging_to_curated_sql", "v1.0")
