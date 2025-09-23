# streamlit_app.py
import streamlit as st
import polars as pl
import plotly.express as px
import tempfile
from app.parse import parse_file_to_parquet
from app.ua import parse_ua, is_bot_ua
from app.agg import hits_by_path, status_distribution, hits_over_time
from lxml import etree

st.set_page_config(page_title="Server log analyzer", layout="wide")

st.title(" Server log analyzer & crawl-budget insights")

uploaded = st.file_uploader(
    "Upload a server log (.log / .gz / .txt)", type=["log", "gz", "txt"]
)

sitemap_file = st.file_uploader(
    "Optional: Upload sitemap.xml for orphan detection", type=["xml"]
)


def parse_sitemap(file):
    tree = etree.parse(file)
    return [loc.text for loc in tree.findall(".//{*}loc")]


if uploaded:
    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=".log") as tmp:
        tmp.write(uploaded.read())
        tmp_path = tmp.name

    # Parse into parquet chunks
    parquet_prefix = tmp_path
    parse_file_to_parquet(tmp_path, parquet_prefix)

    # Load all parquet chunks as one dataset
    df_lazy = pl.scan_parquet(parquet_prefix + "-*.parquet")

    # Add bot classification
    df = (
        df_lazy.collect()
        .with_columns([
            pl.col("user_agent").map_elements(parse_ua, return_dtype=pl.Utf8).alias("bot_family"),
            pl.col("user_agent").map_elements(is_bot_ua, return_dtype=pl.Boolean).alias("is_bot"),
        ])
    )

    # Overview KPIs
    st.subheader("Overview")
    total_hits = df.height
    bot_hits = df.filter(pl.col("is_bot")).height
    pct_bots = (bot_hits / total_hits * 100) if total_hits else 0
    st.metric("Total Requests", f"{total_hits:,}")
    st.metric("Bot Requests", f"{bot_hits:,} ({pct_bots:.1f}%)")

    # Bot Activity
    st.subheader("Bot activity")
    top_bots = (
        df.filter(pl.col("is_bot"))
        .group_by("bot_family")
        .agg(pl.count().alias("hits"))
        .sort("hits", descending=True)
        .limit(10)
    )
    st.dataframe(top_bots.to_pandas())

    # Top Paths with bot breakdown
    st.subheader("Top paths (with bot breakdown)")
    top_paths = (
        df.filter(pl.col("is_bot"))
        .group_by(["path", "bot_family"])
        .agg(pl.count().alias("hits"))
        .sort("hits", descending=True)
        .limit(20)
    )
    st.dataframe(top_paths.to_pandas())

    # Status Code Distribution (horizontal bar)
    st.subheader("Status Code Distribution")
    dist = status_distribution(df.lazy())
    fig = px.bar(dist.to_pandas(), y="status", x="count", orientation="h")
    st.plotly_chart(fig, use_container_width=True)

    # Traffic Over Time (stacked bots vs humans)
    st.subheader("Traffic over time")
    times = (
        df.lazy()
        .with_columns([
            pl.col("time").dt.truncate("1h").alias("bucket"),
            pl.col("is_bot")
        ])
        .group_by(["bucket", "is_bot"])
        .agg(pl.count().alias("hits"))
        .collect()
    )
    fig2 = px.area(times.to_pandas(), x="bucket", y="hits", color="is_bot")
    st.plotly_chart(fig2, use_container_width=True)
    st.dataframe(
        times.rename({"bucket": "Timestamp", "hits": "Visits"}).to_pandas()
    )

    # Slow Endpoints (if request_time column exists)
    if "request_time" in df.columns:
        st.subheader("Slow Endpoints (p95 latency)")
        slow = (
            df.group_by("path")
            .agg(pl.col("request_time").quantile(0.95).alias("p95"))
            .sort("p95", descending=True)
            .limit(20)
        )
        st.dataframe(slow.to_pandas())

    # Top 404s
    st.subheader("Top 404s")
    top_404s = (
        df.filter(pl.col("status") == 404)
        .group_by("path")
        .agg(pl.count().alias("hits"))
        .sort("hits", descending=True)
        .limit(20)
    )
    st.dataframe(top_404s.to_pandas())

    # Orphan Detection (requires sitemap)
    if sitemap_file:
        st.subheader("Orphan detection")
        sitemap_urls = parse_sitemap(sitemap_file)
        sitemap_set = set(sitemap_urls)
        crawled_set = set(df["path"].unique())

        orphans = crawled_set - sitemap_set
        missed = sitemap_set - crawled_set

        st.write("**URLs crawled by bots but not in sitemap (orphans):**")
        st.write(list(orphans)[:20])

        st.write("**URLs in sitemap but never crawled by bots (missed):**")
        st.write(list(missed)[:20])

    # Crawl Budget Optimization Insights
    st.subheader("Crawl budget optimization insights")
    insights = []
    if bot_hits / total_hits > 0.7:
        insights.append("Bots account for most of the traffic — check if unnecessary bots should be blocked in robots.txt.")
    if top_404s.height > 0:
        insights.append("High number of 404s — fix broken links or update sitemap.")
    if "request_time" in df.columns and slow.height > 0:
        insights.append("Some endpoints are very slow — optimize performance to reduce crawl waste.")
    if sitemap_file and missed:
        insights.append("Some sitemap URLs are never crawled — verify they are discoverable and linked.")
    if not insights:
        insights.append("No major crawl budget issues detected.")
    for i in insights:
        st.markdown(f"- {i}")
else:
    st.info("Please upload a log file to begin.")
