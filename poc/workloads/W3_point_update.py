#!/usr/bin/env python3
"""
W3_point_update.py — Benchmark: Point updates (single-row UPDATE by PK).

Tests UPDATE performance and semantics per engine:
  - DuckDB    : Standard SQL UPDATE (MVCC, in-place)
  - Doris     : Requires Unique Key MoW table; UPDATE via DELETE+INSERT or partial update
  - ClickHouse: ALTER TABLE ... UPDATE (mutation) — async by default; we wait for completion

NOTE: Engine semantics differ significantly here. Document gaps, not just latency.

Usage:
    python W3_point_update.py --engine doris --iterations 1000
"""

import argparse
import json
import os
import sys
import time
import random
import numpy as np
from datetime import datetime, timezone
from pathlib import Path

ROOT    = Path(os.environ.get("POC_DIR", "/opt1/olap_poc/poc"))
RESULTS = Path(os.environ.get("RESULTS_DIR", "/opt1/olap_poc/poc/results"))

# event_ids that definitely exist (rows 1..10M)
SAMPLE_IDS = random.sample(range(1, 10_000_001), 5000)


# ---------------------------------------------------------------------------
# Engine implementations
# ---------------------------------------------------------------------------

def point_update_duckdb(env: dict, iterations: int) -> dict:
    """Standard SQL UPDATE by PK — DuckDB supports this natively."""
    import duckdb
    db_path = env.get("DUCKDB_DB_PATH", "/opt1/olap_poc/duckdb/benchmark.duckdb")
    con     = duckdb.connect(db_path)

    latencies = []
    errors    = 0
    rng       = random.Random(77)

    for i in range(iterations):
        eid       = SAMPLE_IDS[i % len(SAMPLE_IDS)]
        new_score = round(rng.uniform(0.01, 9999.99), 2)
        new_lag   = rng.randint(50, 5000)
        sql = f"""
            UPDATE poc.event_fact
            SET    processing_lag_ms = {new_lag},
                   data_version      = 5
            WHERE  event_id = {eid}
        """
        t0 = time.perf_counter()
        try:
            con.execute(sql)
            latencies.append(time.perf_counter() - t0)
        except Exception as exc:
            errors += 1
            latencies.append(None)

    con.close()
    good = [l for l in latencies if l is not None]
    good.sort()
    p95_idx = int(len(good) * 0.95)
    return {
        "status":        "OK",
        "iterations":    iterations,
        "errors":        errors,
        "median_ms":     round(good[len(good)//2] * 1000, 3) if good else None,
        "p95_ms":        round(good[p95_idx] * 1000, 3) if good else None,
        "min_ms":        round(min(good) * 1000, 3) if good else None,
        "max_ms":        round(max(good) * 1000, 3) if good else None,
        "semantic_note": "Standard MVCC UPDATE — fully ACID, in-place.",
    }


def point_update_doris(env: dict, iterations: int) -> dict:
    """
    Doris Unique Key (Merge-on-Write) partial update.
    Requires the event_fact table to be created with UNIQUE KEY + MoW enabled.
    If the table is DUPLICATE KEY, this returns a FEATURE_GAP status.
    """
    try:
        import mysql.connector
    except ImportError:
        return {"status": "FEATURE_GAP", "error": "mysql-connector-python not installed"}

    host = env.get("DORIS_HOST", "127.0.0.1")
    port = int(env.get("DORIS_FE_QUERY_PORT", "9030"))
    user = env.get("DORIS_USER", "root")
    pw   = env.get("DORIS_PASSWORD", "")

    # Retry the initial connection — Doris FE may still be coming up after a
    # previous workload caused it to restart.
    _CONN_RETRIES = 3
    _CONN_DELAY   = 5
    conn = None
    for _attempt in range(1, _CONN_RETRIES + 1):
        try:
            conn = mysql.connector.connect(
                host=host, port=port, user=user, password=pw,
                database="poc", connection_timeout=10,
            )
            break
        except Exception as _exc:
            if _attempt == _CONN_RETRIES:
                return {"status": "ERROR", "error": str(_exc)}
            time.sleep(_CONN_DELAY)

    try:
        cur  = conn.cursor()

        # Check if table supports UPDATE (Unique Key MoW)
        cur.execute("SHOW CREATE TABLE event_fact")
        ddl = str(cur.fetchone())
        if "UNIQUE KEY" not in ddl:
            cur.close(); conn.close()
            return {
                "status": "FEATURE_GAP",
                "semantic_note": (
                    "Doris DUPLICATE KEY tables do not support row-level UPDATE. "
                    "Use event_fact_mow (UNIQUE KEY + enable_unique_key_merge_on_write=true). "
                    "Partial update via HTTP stream load with __DORIS_DELETE_SIGN__ is the idiomatic path."
                ),
            }

        latencies = []
        errors    = 0
        rng       = random.Random(77)

        for i in range(iterations):
            eid     = SAMPLE_IDS[i % len(SAMPLE_IDS)]
            new_lag = rng.randint(50, 5000)
            sql = f"""
                UPDATE event_fact
                SET    processing_lag_ms = {new_lag},
                       data_version      = 5
                WHERE  event_id = {eid}
            """
            t0 = time.perf_counter()
            try:
                cur.execute(sql)
                conn.commit()
                latencies.append(time.perf_counter() - t0)
            except Exception as exc:
                errors += 1
                latencies.append(None)

        cur.close(); conn.close()

        good = [l for l in latencies if l is not None]
        good.sort()
        p95_idx = int(len(good) * 0.95)
        return {
            "status":        "OK",
            "iterations":    iterations,
            "errors":        errors,
            "median_ms":     round(good[len(good)//2] * 1000, 3) if good else None,
            "p95_ms":        round(good[p95_idx] * 1000, 3) if good else None,
            "semantic_note": "Doris Unique Key MoW partial UPDATE — synchronous write path.",
        }
    except Exception as exc:
        return {"status": "ERROR", "error": str(exc)}


def point_update_clickhouse(env: dict, iterations: int) -> dict:
    """
    ClickHouse ALTER TABLE ... UPDATE mutation.
    Mutations are ASYNC — we measure submission latency + poll for completion.
    This is a fundamental semantic difference: document it clearly.
    """
    import requests

    host   = env.get("CLICKHOUSE_HOST", "127.0.0.1")
    port   = env.get("CLICKHOUSE_HTTP_PORT", "8123")
    user   = env.get("CLICKHOUSE_USER", "default")
    passwd = env.get("CLICKHOUSE_PASSWORD", "")
    db     = env.get("CLICKHOUSE_DATABASE", "poc")
    auth   = (user, passwd) if passwd else (user, "")
    base   = f"http://{host}:{port}/"

    submission_latencies = []
    completion_latencies = []
    errors = 0
    rng    = random.Random(77)

    for i in range(iterations):
        eid     = SAMPLE_IDS[i % len(SAMPLE_IDS)]
        new_lag = rng.randint(50, 5000)
        sql = (
            f"ALTER TABLE {db}.event_fact "
            f"UPDATE processing_lag_ms = {new_lag}, data_version = 5 "
            f"WHERE event_id = {eid}"
        )
        t0 = time.perf_counter()
        resp = requests.post(base, params={"query": sql}, auth=auth, timeout=30)
        submit_elapsed = time.perf_counter() - t0

        if resp.status_code != 200:
            errors += 1
            continue

        submission_latencies.append(submit_elapsed)

        # Poll for mutation completion (up to 30s)
        mutation_done  = False
        poll_start     = time.perf_counter()
        for _ in range(60):
            time.sleep(0.5)
            check = requests.get(
                base,
                params={"query":
                    f"SELECT is_done FROM system.mutations "
                    f"WHERE database='{db}' AND table='event_fact' "
                    f"AND is_done=0 ORDER BY create_time DESC LIMIT 1 FORMAT TSV"},
                auth=auth, timeout=10,
            )
            if check.text.strip() == "":
                mutation_done = True
                break
        completion_latencies.append(time.perf_counter() - poll_start)

        if not mutation_done:
            errors += 1

    sl = sorted(submission_latencies)
    cl = sorted(completion_latencies)
    p95 = int(max(len(sl) * 0.95, 0))

    return {
        "status":             "OK" if errors == 0 else "PARTIAL",
        "iterations":         iterations,
        "errors":             errors,
        "submit_median_ms":   round(sl[len(sl)//2] * 1000, 3) if sl else None,
        "submit_p95_ms":      round(sl[p95] * 1000, 3) if sl else None,
        "complete_median_ms": round(cl[len(cl)//2] * 1000, 3) if cl else None,
        "complete_p95_ms":    round(cl[p95] * 1000, 3) if cl else None,
        "semantic_note": (
            "ClickHouse mutations are ASYNCHRONOUS. Submission is fast (~ms); "
            "completion rewrites entire data parts and can take seconds to minutes. "
            "Point updates are an anti-pattern in ClickHouse — use ReplacingMergeTree instead."
        ),
    }


UPDATERS = {
    "doris":      point_update_doris,
    "duckdb":     point_update_duckdb,
    "clickhouse": point_update_clickhouse,
}


def main():
    parser = argparse.ArgumentParser(description="W3 — Point update benchmark")
    parser.add_argument("--engine",     required=True, choices=list(UPDATERS))
    parser.add_argument("--iterations", type=int, default=1000)
    args = parser.parse_args()

    env = {**os.environ}
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                env.setdefault(k.strip(), v.strip())

    print(f"W3 Point Update — engine={args.engine}  iterations={args.iterations:,}")
    t0     = time.perf_counter()
    result = UPDATERS[args.engine](env, args.iterations)
    total  = time.perf_counter() - t0

    record = {
        "workload":    "W3_point_update",
        "engine":      args.engine,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "total_s":     round(total, 3),
        **result,
    }
    print(json.dumps(record, indent=2))

    RESULTS.mkdir(parents=True, exist_ok=True)
    out = RESULTS / f"W3_{args.engine}_{int(time.time())}.jsonl"
    out.write_text(json.dumps(record) + "\n")

    if result.get("status") == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
