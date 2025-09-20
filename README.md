\# Server Log Analyzer \& Crawl-Budget Insights



A demo web app built with Python 3.11 + Streamlit to analyze raw server logs for crawl-budget insights.



\## Features

\- Parse Apache/Nginx logs (supports `.gz`).

\- Bot detection (via UA + regex).

\- Aggregations (DuckDB + Polars).

\- Visualizations (Streamlit + Plotly).

\- CLI preprocessor for large logs → Parquet.



\## Quickstart



\### 1. Install dependencies

Using `uv` (recommended):



```powershell

uv init

uv add regex ua-parser user-agents polars pandas duckdb pyarrow tldextract plotly streamlit typer lxml psycopg2-binary

