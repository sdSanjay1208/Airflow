import streamlit as st
import pandas as pd
import plotly.express as px

from api_client import (
    fetch_latest_status,
    fetch_pipeline_history,
    fetch_dq_scores
)

st.set_page_config(page_title="ETL Monitoring Dashboard", layout="wide")

st.title("📊 Enterprise ETL Monitoring Dashboard")

# ---------------------------
# Latest Pipeline Status
# ---------------------------
st.subheader("✅ Latest Pipeline Run")

status = fetch_latest_status()

st.metric(
    label="Pipeline Status",
    value=status["status"],
    delta=str(status["end_time"])
)

# ---------------------------
# Run History View
# ---------------------------
st.subheader("📜 Pipeline Run History")

history_df = pd.DataFrame(fetch_pipeline_history())
st.dataframe(history_df, use_container_width=True)

# ---------------------------
# DQ Score Visualization
# ---------------------------
st.subheader("🧪 Data Quality / Volume Trend")

dq_df = pd.DataFrame(fetch_dq_scores())

fig = px.line(
    dq_df,
    x="run_date",
    y="total_records",
    title="Total Records Loaded Over Time",
    markers=True
)

st.plotly_chart(fig, use_container_width=True)

# ---------------------------
# Trend Charts
# ---------------------------
st.subheader("📈 Status Distribution Trend")

trend = history_df.groupby("status").size().reset_index(name="count")

fig2 = px.bar(
    trend,
    x="status",
    y="count",
    title="Pipeline Run Status Distribution"
)

st.plotly_chart(fig2, use_container_width=True)
