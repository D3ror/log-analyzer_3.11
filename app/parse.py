# app/parse.py
import re
import gzip
from datetime import datetime
import polars as pl

LOG_RE = re.compile(
    r'(?P<remote_addr>\S+) \S+ (?P<remote_user>\S+) \[(?P<time_local>[^\]]+)\] '
    r'"(?P<request>[^"]*)" (?P<status>\d{3}) (?P<body_bytes_sent>\S+) '
    r'"(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)"'
)

TIME_FMT = "%d/%b/%Y:%H:%M:%S %z"


def parse_request(request: str):
    try:
        method, path, proto = request.split()
    except ValueError:
        method, path, proto = None, request, None
    return method, path, proto


def iter_lines(path: str):
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            yield line


def parse_file_to_parquet(in_path: str, out_parquet: str, batch_size: int = 50_000):
    """
    Parse an Apache/Nginx log file into a Parquet file (streaming).
    """
    batch = []
    idx = 0
    for i, line in enumerate(iter_lines(in_path), 1):
        m = LOG_RE.match(line)
        if not m:
            continue
        d = m.groupdict()
        method, path, proto = parse_request(d["request"])
        try:
            ts = datetime.strptime(d["time_local"], TIME_FMT)
        except Exception:
            ts = None
        row = {
            "remote_addr": d["remote_addr"],
            "time": ts,
            "method": method,
            "path": path,
            "status": int(d["status"]),
            "bytes": int(d["body_bytes_sent"]) if d["body_bytes_sent"].isdigit() else None,
            "referer": d["http_referer"],
            "user_agent": d["http_user_agent"],
        }
        batch.append(row)
        if len(batch) >= batch_size:
            df = pl.DataFrame(batch)
            df.write_parquet(out_parquet if idx == 0 else out_parquet, append=True)
            batch = []
            idx += 1
    if batch:
        df = pl.DataFrame(batch)
        df.write_parquet(out_parquet if idx == 0 else out_parquet, append=True)
