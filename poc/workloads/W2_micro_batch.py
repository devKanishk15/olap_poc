#!/usr/bin/env python3
"""
W2_micro_batch.py — Benchmark: Micro-batch inserts (10k rows × N batches).

Simulates a streaming ingest pattern: small batches arriving repeatedly.
Measures per-batch latency, total throughput, and tail latency (p95).

Usage:
    python W2_micro_batch.py --engine doris --batches 50 --batch-size 10000
"""

import argparse
import json
import os
import sys
import time
import struct
import hashlib
import numpy as np
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

ROOT     = Path(os.environ.get("POC_DIR", "/opt1/poc"))
RESULTS  = Path(os.environ.get("RESULTS_DIR", "/opt1/poc/results"))
SEED     = 99   # different seed from generator to simulate new data


def generate_micro_batch(rng: np.random.Generator, batch_size: int, batch_idx: int) -> list[dict]:
    """Generate batch_size rows as a list of dicts."""
    base_id   = 10_000_000 + batch_idx * batch_size
    event_date = date(2024, 1, 30)  # new day beyond the generated dataset

    rows = []
    for i in range(batch_size):
        eid = base_id + i
        uid = int(rng.integers(1, 500_001))
        rows.append({
            "event_id":               eid,
            "event_date":             str(event_date),
            "event_ts":               f"2024-01-30 {rng.integers(0,24):02d}:{rng.integers(0,60):02d}:{rng.integers(0,60):02d}.000000",
            "session_id":             f"{uid:08x}-{eid:012x}",
            "user_id":                uid,
            "device_id":              f"dev-{rng.integers(1,600001):010x}",
            "event_type":             rng.choice(["click","view","purchase","scroll"]),
            "event_subtype":          f"subtype_{rng.integers(0,200):03d}",
            "page_id":                int(rng.integers(1, 5001)),
            "page_name":              f"page_{rng.integers(1,5001)}",
            "referrer_url":           None,
            "campaign_id":            None,
            "campaign_channel":       rng.choice(["direct","organic","email"]),
            "ab_variant":             rng.choice(["A","B","control"]),
            "country_code":           rng.choice(["US","GB","DE","IN","JP"]),
            "region":                 "region_US_1",
            "city":                   f"city_{rng.integers(1,800)}",
            "latitude":               float(rng.uniform(-90,90)),
            "longitude":              float(rng.uniform(-180,180)),
            "ip_address":             f"10.{rng.integers(0,256)}.{rng.integers(0,256)}.{rng.integers(1,256)}",
            "user_agent":             f"Mozilla/5.0 Chrome/{rng.integers(80,120)}",
            "os_family":              rng.choice(["Windows","macOS","Android"]),
            "browser_family":         rng.choice(["Chrome","Safari","Edge"]),
            "device_type":            rng.choice(["desktop","mobile"]),
            "screen_width":           int(rng.choice([1280,1440,375,390])),
            "screen_height":          int(rng.choice([720,900,667,812])),
            "viewport_width":         1280,
            "viewport_height":        720,
            "product_id":             None,
            "product_name":           None,
            "product_category_l1":    None,
            "product_category_l2":    None,
            "product_price":          None,
            "quantity":               None,
            "order_id":               None,
            "order_total":            None,
            "discount_amount":        None,
            "coupon_code":            None,
            "revenue":                None,
            "duration_ms":            int(rng.integers(100, 30001)),
            "scroll_depth_pct":       int(rng.integers(0, 101)),
            "click_x":                int(rng.integers(0, 1921)),
            "click_y":                int(rng.integers(0, 1081)),
            "is_bot":                 False,
            "is_authenticated":       bool(rng.random() < 0.5),
            "is_first_visit":         bool(rng.random() < 0.1),
            "experiment_id":          None,
            "server_id":              int(rng.integers(1, 33)),
            "load_time_ms":           int(rng.integers(50, 5001)),
            "ttfb_ms":                int(rng.integers(10, 500)),
            "error_code":             None,
            "error_message":          None,
            "tag_list":               None,
            "custom_dimensions":      '{"plan":"free","theme":"dark","locale":"en","cohort":"A","flag":"false"}',
            "raw_payload_size_bytes": int(rng.integers(100, 5001)),
            "ingestion_ts":           f"2024-01-30 {rng.integers(0,24):02d}:{rng.integers(0,60):02d}:{rng.integers(0,60):02d}.000",
            "processing_lag_ms":      int(rng.integers(50, 2001)),
            "data_version":           1,
            "partition_key":          eid % 30,
            "checksum":               int(eid * uid),
        })
    return rows


def insert_doris(rows: list[dict], env: dict) -> float:
    """Insert via MySQL protocol using mysql-connector."""
    import mysql.connector
    host  = env.get("DORIS_HOST", "127.0.0.1")
    port  = int(env.get("DORIS_FE_QUERY_PORT", "9030"))
    user  = env.get("DORIS_USER", "root")
    pw    = env.get("DORIS_PASSWORD", "")

    conn = mysql.connector.connect(host=host, port=port, user=user, password=pw, database="poc")
    cur  = conn.cursor()

    cols = list(rows[0].keys())
    placeholders = ",".join(["%s"] * len(cols))
    sql  = f"INSERT INTO event_fact ({','.join(cols)}) VALUES ({placeholders})"
    vals = [tuple(r[c] for c in cols) for r in rows]

    t0 = time.perf_counter()
    cur.executemany(sql, vals)
    conn.commit()
    elapsed = time.perf_counter() - t0

    cur.close()
    conn.close()
    return elapsed


def insert_duckdb(rows: list[dict], env: dict) -> float:
    import duckdb
    db_path = env.get("DUCKDB_DB_PATH", "/opt1/duckdb/benchmark.duckdb")
    con     = duckdb.connect(db_path)

    import pandas as pd
    df = pd.DataFrame(rows)

    t0 = time.perf_counter()
    con.execute("INSERT INTO poc.event_fact SELECT * FROM df")
    elapsed = time.perf_counter() - t0

    con.close()
    return elapsed


def insert_clickhouse(rows: list[dict], env: dict) -> float:
    import requests, io, csv
    host   = env.get("CLICKHOUSE_HOST", "127.0.0.1")
    port   = env.get("CLICKHOUSE_HTTP_PORT", "8123")
    user   = env.get("CLICKHOUSE_USER", "default")
    passwd = env.get("CLICKHOUSE_PASSWORD", "")
    db     = env.get("CLICKHOUSE_DATABASE", "poc")

    cols = list(rows[0].keys())
    buf  = io.StringIO()
    w    = csv.DictWriter(buf, fieldnames=cols)
    for r in rows:
        w.writerow(r)
    csv_data = buf.getvalue().encode()

    query = f"INSERT INTO {db}.event_fact ({','.join(cols)}) FORMAT CSV"
    auth  = (user, passwd) if passwd else (user, "")
    t0    = time.perf_counter()
    resp  = requests.post(
        f"http://{host}:{port}/",
        params={"query": query},
        data=csv_data,
        headers={"Content-Type": "text/plain"},
        auth=auth,
        timeout=120,
    )
    elapsed = time.perf_counter() - t0
    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse insert failed: {resp.text}")
    return elapsed


INSERTERS = {"doris": insert_doris, "duckdb": insert_duckdb, "clickhouse": insert_clickhouse}


def main():
    parser = argparse.ArgumentParser(description="W2 — Micro-batch insert benchmark")
    parser.add_argument("--engine",     required=True, choices=list(INSERTERS))
    parser.add_argument("--batches",    type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=10_000)
    args = parser.parse_args()

    env = {**os.environ}
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                env.setdefault(k.strip(), v.strip())

    rng     = np.random.default_rng(SEED)
    latencies = []

    print(f"W2 Micro-batch — engine={args.engine}  batches={args.batches}  batch_size={args.batch_size:,}")

    t_total = time.perf_counter()
    for b in range(args.batches):
        rows    = generate_micro_batch(rng, args.batch_size, batch_idx=b)
        t_batch = INSERTERS[args.engine](rows, env)
        latencies.append(t_batch)
        rate = args.batch_size / t_batch if t_batch > 0 else 0
        print(f"  batch {b+1:>4}/{args.batches}  {t_batch*1000:.1f} ms  {rate:,.0f} rows/s", end="\r")

    total_elapsed = time.perf_counter() - t_total
    total_rows    = args.batches * args.batch_size
    lat_arr       = sorted(latencies)
    p95_idx       = int(len(lat_arr) * 0.95)

    result = {
        "workload":      "W2_micro_batch",
        "engine":        args.engine,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "batches":       args.batches,
        "batch_size":    args.batch_size,
        "total_rows":    total_rows,
        "total_s":       round(total_elapsed, 3),
        "rows_per_s":    round(total_rows / total_elapsed),
        "median_batch_ms": round(lat_arr[len(lat_arr)//2] * 1000, 2),
        "p95_batch_ms":    round(lat_arr[p95_idx] * 1000, 2),
        "min_batch_ms":    round(min(latencies) * 1000, 2),
        "max_batch_ms":    round(max(latencies) * 1000, 2),
        "status":          "OK",
    }

    print(f"\n{json.dumps(result, indent=2)}")
    RESULTS.mkdir(parents=True, exist_ok=True)
    out = RESULTS / f"W2_{args.engine}_{int(time.time())}.jsonl"
    out.write_text(json.dumps(result) + "\n")


if __name__ == "__main__":
    main()
