import sys
sys.path.append("/opt/airflow/scripts")

from metadata_api import (
    get_latest_pipeline_status,
    get_pipeline_history,
    get_dq_scores
)

def fetch_latest_status():
    return get_latest_pipeline_status()

def fetch_pipeline_history():
    return get_pipeline_history()

def fetch_dq_scores():
    return get_dq_scores()
