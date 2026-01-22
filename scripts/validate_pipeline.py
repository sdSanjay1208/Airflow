import mysql.connector
from metadata_logger import log_task


def validate_pipeline():
    try:
        conn = mysql.connector.connect(
            host="host.docker.internal",
            user="airflow_user",
            password="airflow_pass",
            database="etl_project"
        )
        cursor = conn.cursor()

        checks = {
            "raw_count": "SELECT COUNT(*) FROM raw_amazon",
            "stg_count": "SELECT COUNT(*) FROM stg_amazon",
            "cur_count": "SELECT COUNT(*) FROM cur_amazon",
            "null_order_id": "SELECT COUNT(*) FROM stg_amazon WHERE order_id IS NULL",
            "null_total_amount": "SELECT COUNT(*) FROM stg_amazon WHERE total_amount IS NULL"
        }

        results = {}

        for name, query in checks.items():
            cursor.execute(query)
            results[name] = cursor.fetchone()[0]

        print("🔍 ETL Validation Results")
        for k, v in results.items():
            print(f"{k}: {v}")

        # -------------------------
        # BUSINESS RULE VALIDATION
        # -------------------------
        if results["raw_count"] < results["stg_count"]:
            raise ValueError("STAGING has more rows than RAW")

        if results["null_order_id"] > 0:
            raise ValueError("NULL order_id found in STAGING")

        if results["null_total_amount"] > 0:
            raise ValueError("NULL total_amount found in STAGING")

        # -------------------------
        # METADATA LOGGING
        # -------------------------
        log_task(
            "amazon_etl_pipeline",
            "validate_pipeline",
            "SUCCESS",
            read=results["stg_count"],
            written=results["cur_count"]
        )

        print("✅ Data validation passed")

        cursor.close()
        conn.close()

    except Exception as e:
        log_task(
            "amazon_etl_pipeline",
            "validate_pipeline",
            "FAILED",
            error=str(e)
        )
        raise
