# streamlit_app.py
import streamlit as st
import polars as pl
import plotly.express as px
from app.parse import parse_file_to_parquet
from app.agg import hits_by_path, status_distribution, hits_over_time
import tempfile

st.set_page_config(page_title="Server Log Analyzer", layout="wide")

st.title("📊 Server Log Analyzer & Crawl-Budget Insights")

uploaded = st.file_uploader("Upload a server log (.log / .gz)", type=["log", "gz", "txt"])

if uploaded:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as tmp:
        tmp.write(uploaded.read())
        tmp_path = tmp.name

    parquet_file = tmp_path + ".parquet"
    parse_file_to_parquet(tmp_path, parquet_file)

    df_lazy = pl.scan_parquet(parquet_file)

    st.subheader("Top Paths")
    top_paths = hits_by_path(df_lazy, 20)
    st.dataframe(top_paths.to_pandas())

    st.subheader("Status Code Distribution")
    dist = status_distribution(df_lazy)
    fig = px.bar(dist.to_pandas(), x="status", y="count")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Traffic Over Time")
    times = hits_over_time(df_lazy)
    fig2 = px.line(times.to_pandas(), x="bucket", y="hits")
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("Please upload a log file to begin.")
