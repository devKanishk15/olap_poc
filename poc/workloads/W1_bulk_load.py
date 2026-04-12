#!/usr/bin/env python3
"""
W1_bulk_load.py — Benchmark: Bulk load of all 10M rows into each engine.

Measures wall-time for ingesting the full dataset from local Parquet files.
Run via the harness (run_benchmark.py) or standalone:
    python W1_bulk_load.py --engine doris
    python W1_bulk_load.py --engine duckdb
    python W1_bulk_load.py --engine clickhouse
"""

import argparse
import os
import sys
import time
import json
import subprocess
import glob
from pathlib import Path
from datetime import datetime, timezone

ROOT      = Path(os.environ.get("POC_DIR", "/opt1/poc"))
DATA_DIR  = Path(os.environ.get("DATA_DIR", "/opt1/data"))
RESULTS   = Path(os.environ.get("RESULTS_DIR", "/opt1/poc/results"))


# ---------------------------------------------------------------------------
# Engine-specific loaders
# ---------------------------------------------------------------------------

def bulk_load_doris(env: dict) -> dict:
    """Stream-load all Parquet partition files via Doris HTTP stream load API."""
    import requests

    host    = env.get("DORIS_HOST", "127.0.0.1")
    http    = env.get("DORIS_FE_HTTP_PORT", "8030")
    user    = env.get("DORIS_USER", "root")
    passwd  = env.get("DORIS_PASSWORD", "")
    db      = "poc"
    table   = "event_fact"

    parquet_files = sorted(DATA_DIR.rglob("event_fact/**/*.parquet"))
    if not parquet_files:
        return {"status": "ERROR", "error": f"No parquet files under {DATA_DIR}"}

    url = f"http://{host}:{http}/api/{db}/{table}/_stream_load"

    rows_loaded = 0
    files_loaded = 0
    t_start = time.perf_counter()

    for pf in parquet_files:
        label = f"bulk_load_{pf.stem}_{int(time.time())}"
        headers = {
            "label":          label,
            "format":         "parquet",
            "where":          "",
            "max_filter_ratio": "0.01",
            "Expect":         "100-continue",
        }
        with open(pf, "rb") as f:
            resp = requests.put(
                url,
                data=f,
                headers=headers,
                auth=(user, passwd),
                timeout=300,
            )
        result = resp.json()
        if result.get("Status") not in ("Success", "Publish Timeout"):
            return {"status": "ERROR", "error": str(result), "files_loaded": files_loaded}
        rows_loaded  += int(result.get("NumberLoadedRows", 0))
        files_loaded += 1

    elapsed = time.perf_counter() - t_start
    return {
        "status":       "OK",
        "rows_loaded":  rows_loaded,
        "files_loaded": files_loaded,
        "elapsed_s":    round(elapsed, 3),
        "rows_per_s":   round(rows_loaded / elapsed) if elapsed > 0 else 0,
    }


def bulk_load_duckdb(env: dict) -> dict:
    """INSERT INTO ... SELECT * FROM read_parquet(glob)."""
    import duckdb

    db_path = env.get("DUCKDB_DB_PATH", "/opt1/duckdb/benchmark.duckdb")
    parquet_glob = str(DATA_DIR / "event_fact" / "**" / "*.parquet")

    con = duckdb.connect(db_path)
    con.execute("SET memory_limit = '6GB'")
    con.execute(f"SET temp_directory = '/opt1/duckdb/spill'")
    con.execute("SET threads = 4")
    con.execute("LOAD parquet")

    # Truncate first
    con.execute("DELETE FROM poc.event_fact")

    t_start = time.perf_counter()
    con.execute(f"""
        INSERT INTO poc.event_fact
        SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)
    """)
    elapsed = time.perf_counter() - t_start

    row_count = con.execute("SELECT COUNT(*) FROM poc.event_fact").fetchone()[0]
    con.close()

    return {
        "status":      "OK",
        "rows_loaded": row_count,
        "elapsed_s":   round(elapsed, 3),
        "rows_per_s":  round(row_count / elapsed) if elapsed > 0 else 0,
    }


def bulk_load_clickhouse(env: dict) -> dict:
    """INSERT INTO ... SELECT * FROM file() or clickhouse-client --query with local files."""
    import requests

    host   = env.get("CLICKHOUSE_HOST", "127.0.0.1")
    port   = env.get("CLICKHOUSE_HTTP_PORT", "8123")
    user   = env.get("CLICKHOUSE_USER", "default")
    passwd = env.get("CLICKHOUSE_PASSWORD", "")
    db     = env.get("CLICKHOUSE_DATABASE", "poc")

    parquet_files = sorted(DATA_DIR.rglob("event_fact/**/*.parquet"))
    if not parquet_files:
        return {"status": "ERROR", "error": f"No parquet files under {DATA_DIR}"}

    base_url = f"http://{host}:{port}/"
    auth     = (user, passwd) if passwd else (user, "")

    # Truncate
    requests.post(base_url, params={"query": f"TRUNCATE TABLE {db}.event_fact"}, auth=auth, timeout=60)

    rows_loaded  = 0
    files_loaded = 0
    t_start      = time.perf_counter()

    for pf in parquet_files:
        query = f"INSERT INTO {db}.event_fact FORMAT Parquet"
        with open(pf, "rb") as f:
            resp = requests.post(
                base_url,
                params={"query": query},
                data=f,
                headers={"Content-Type": "application/octet-stream"},
                auth=auth,
                timeout=300,
            )
        if resp.status_code != 200:
            return {"status": "ERROR", "error": resp.text, "files_loaded": files_loaded}
        files_loaded += 1

    elapsed = time.perf_counter() - t_start

    # Row count
    resp = requests.get(base_url, params={"query": f"SELECT count() FROM {db}.event_fact FORMAT TSV"}, auth=auth)
    rows_loaded = int(resp.text.strip()) if resp.status_code == 200 else -1

    return {
        "status":       "OK",
        "rows_loaded":  rows_loaded,
        "files_loaded": files_loaded,
        "elapsed_s":    round(elapsed, 3),
        "rows_per_s":   round(rows_loaded / elapsed) if elapsed > 0 and rows_loaded > 0 else 0,
    }


LOADERS = {
    "doris":      bulk_load_doris,
    "duckdb":     bulk_load_duckdb,
    "clickhouse": bulk_load_clickhouse,
}


def main():
    parser = argparse.ArgumentParser(description="W1 — Bulk load benchmark")
    parser.add_argument("--engine", required=True, choices=list(LOADERS))
    args = parser.parse_args()

    # Load .env
    env = {**os.environ}
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                env.setdefault(k.strip(), v.strip())

    print(f"W1 Bulk Load — engine={args.engine}  data={DATA_DIR}")
    t0     = time.perf_counter()
    result = LOADERS[args.engine](env)
    total  = time.perf_counter() - t0

    record = {
        "workload":     "W1_bulk_load",
        "engine":       args.engine,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "wall_time_s":  round(total, 3),
        **result,
    }

    RESULTS.mkdir(parents=True, exist_ok=True)
    out_file = RESULTS / f"W1_{args.engine}_{int(time.time())}.jsonl"
    out_file.write_text(json.dumps(record) + "\n")

    print(json.dumps(record, indent=2))
    if result.get("status") != "OK":
        sys.exit(1)


if __name__ == "__main__":
    main()
