# app/agg.py
import polars as pl
import duckdb


def load_logs_lazy(parquet_path: str):
    """Return a Polars LazyFrame over logs parquet(s)."""
    return pl.scan_parquet(parquet_path)


def hits_by_path(df: pl.LazyFrame, top_n: int = 20):
    return (
        df.group_by("path")
        .agg(pl.count().alias("hits"))
        .sort("hits", descending=True)
        .limit(top_n)
        .collect()
    )


def status_distribution(df: pl.LazyFrame):
    return (
        df.group_by("status")
        .agg(pl.count().alias("count"))
        .sort("status")
        .collect()
    )


def hits_over_time(df: pl.LazyFrame, freq: str = "1h"):
    return (
        df.with_columns(pl.col("time").dt.truncate(freq).alias("bucket"))
        .group_by("bucket")
        .agg(pl.count().alias("hits"))
        .sort("bucket")
        .collect()
    )


def connect_duckdb(parquet_path: str):
    con = duckdb.connect()
    con.execute(f"CREATE VIEW logs AS SELECT * FROM read_parquet('{parquet_path}')")
    return con
