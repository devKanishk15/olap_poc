#!/usr/bin/env python3
"""
W4_bulk_update.py — Benchmark: Bulk UPDATE touching ~5% of all rows (WHERE clause).

Simulates a real-world pattern: re-scoring or re-tagging a segment of data.
Target: ~500k rows (5% of 10M).

  - DuckDB    : Standard UPDATE ... WHERE  (MVCC)
  - Doris     : UPDATE ... WHERE on Unique Key MoW; documents gap on Duplicate Key
  - ClickHouse: ALTER TABLE ... UPDATE (mutation) — async, rewrites part files

Usage:
    python W4_bulk_update.py --engine doris
    python W4_bulk_update.py --engine duckdb
    python W4_bulk_update.py --engine clickhouse
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT    = Path(os.environ.get("POC_DIR", "/opt1/olap_poc/poc"))
RESULTS = Path(os.environ.get("RESULTS_DIR", "/opt1/olap_poc/poc/results"))

# Target segment: server_id IN (1..16) → ~50% of servers → ~5M rows
# For ~5% touch rate, use country_code = 'US' (≈ 1/60 countries × adjusted dist = ~8%)
BULK_UPDATE_WHERE = "country_code = 'US' AND ab_variant = 'A'"
EXPECTED_TOUCH_PCT = "~5%"


def bulk_update_duckdb(env: dict) -> dict:
    import duckdb
    db_path = env.get("DUCKDB_DB_PATH", "/opt1/olap_poc/duckdb/benchmark.duckdb")
    con     = duckdb.connect(db_path)

    sql = f"""
        UPDATE poc.event_fact
        SET    data_version      = 5,
               processing_lag_ms = processing_lag_ms + 1
        WHERE  {BULK_UPDATE_WHERE}
    """
    t0      = time.perf_counter()
    con.execute(sql)
    elapsed = time.perf_counter() - t0

    rows_affected = con.execute(
        f"SELECT COUNT(*) FROM poc.event_fact WHERE {BULK_UPDATE_WHERE}"
    ).fetchone()[0]
    con.close()

    return {
        "status":        "OK",
        "rows_affected": rows_affected,
        "elapsed_s":     round(elapsed, 3),
        "rows_per_s":    round(rows_affected / elapsed) if elapsed > 0 else 0,
        "semantic_note": "Standard MVCC bulk UPDATE — ACID, full predicate pushdown.",
    }


def bulk_update_doris(env: dict) -> dict:
    try:
        import mysql.connector
    except ImportError:
        return {"status": "ERROR", "error": "mysql-connector-python not installed"}

    host = env.get("DORIS_HOST", "127.0.0.1")
    port = int(env.get("DORIS_FE_QUERY_PORT", "9030"))
    user = env.get("DORIS_USER", "root")
    pw   = env.get("DORIS_PASSWORD", "")

    try:
        conn = mysql.connector.connect(host=host, port=port, user=user, password=pw, database="poc")
        cur  = conn.cursor()

        cur.execute("SHOW CREATE TABLE event_fact")
        ddl = str(cur.fetchone())

        if "UNIQUE KEY" not in ddl:
            cur.close(); conn.close()
            return {
                "status": "FEATURE_GAP",
                "semantic_note": (
                    "Doris DUPLICATE KEY model does not support UPDATE statements. "
                    "Bulk updates require: (a) switch to UNIQUE KEY MoW table, or "
                    "(b) INSERT OVERWRITE with full partition replacement, or "
                    "(c) stream load with __DORIS_DELETE_SIGN__ column for delete+reinsert. "
                    "This is a material operational gap vs DuckDB/ClickHouse."
                ),
            }

        sql = f"""
            UPDATE event_fact
            SET    data_version      = 5,
                   processing_lag_ms = processing_lag_ms + 1
            WHERE  {BULK_UPDATE_WHERE}
        """
        t0 = time.perf_counter()
        cur.execute(sql)
        conn.commit()
        elapsed = time.perf_counter() - t0

        cur.execute(f"SELECT COUNT(*) FROM event_fact WHERE {BULK_UPDATE_WHERE}")
        rows_affected = cur.fetchone()[0]
        cur.close(); conn.close()

        return {
            "status":        "OK",
            "rows_affected": rows_affected,
            "elapsed_s":     round(elapsed, 3),
            "rows_per_s":    round(rows_affected / elapsed) if elapsed > 0 else 0,
            "semantic_note": "Doris Unique Key MoW bulk UPDATE — synchronous, rewrites affected rows.",
        }
    except Exception as exc:
        return {"status": "ERROR", "error": str(exc)}


def bulk_update_clickhouse(env: dict) -> dict:
    import requests

    host   = env.get("CLICKHOUSE_HOST", "127.0.0.1")
    port   = env.get("CLICKHOUSE_HTTP_PORT", "8123")
    user   = env.get("CLICKHOUSE_USER", "default")
    passwd = env.get("CLICKHOUSE_PASSWORD", "")
    db     = env.get("CLICKHOUSE_DATABASE", "poc")
    auth   = (user, passwd) if passwd else (user, "")
    base   = f"http://{host}:{port}/"

    # Count affected rows first
    r = requests.get(base, params={
        "query": f"SELECT count() FROM {db}.event_fact WHERE {BULK_UPDATE_WHERE} FORMAT TSV"
    }, auth=auth, timeout=60)
    rows_affected = int(r.text.strip()) if r.status_code == 200 else -1

    sql = (
        f"ALTER TABLE {db}.event_fact "
        f"UPDATE data_version = 5, "
        f"processing_lag_ms = processing_lag_ms + 1 "
        f"WHERE {BULK_UPDATE_WHERE}"
    )

    t_submit = time.perf_counter()
    resp = requests.post(base, params={"query": sql}, auth=auth, timeout=60)
    submit_elapsed = time.perf_counter() - t_submit

    if resp.status_code != 200:
        return {"status": "ERROR", "error": resp.text}

    # Poll for mutation completion
    t_poll = time.perf_counter()
    mutation_done = False
    for _ in range(120):   # up to 60s
        time.sleep(0.5)
        check = requests.get(base, params={
            "query": (
                f"SELECT count() FROM system.mutations "
                f"WHERE database='{db}' AND table='event_fact' AND is_done=0 FORMAT TSV"
            )
        }, auth=auth, timeout=10)
        if check.text.strip() == "0" or check.text.strip() == "":
            mutation_done = True
            break
    completion_elapsed = time.perf_counter() - t_poll

    return {
        "status":               "OK" if mutation_done else "TIMEOUT",
        "rows_affected":        rows_affected,
        "submit_elapsed_s":     round(submit_elapsed, 3),
        "completion_elapsed_s": round(completion_elapsed, 3),
        "total_elapsed_s":      round(submit_elapsed + completion_elapsed, 3),
        "rows_per_s":           round(rows_affected / (submit_elapsed + completion_elapsed))
                                if (submit_elapsed + completion_elapsed) > 0 and rows_affected > 0 else 0,
        "semantic_note": (
            "ClickHouse ALTER TABLE ... UPDATE is an ASYNC mutation that rewrites affected part files. "
            f"Submit latency={round(submit_elapsed*1000)}ms (fast); "
            f"Completion={round(completion_elapsed, 1)}s (rewrites parts on disk). "
            "For high-frequency updates at scale, prefer ReplacingMergeTree + periodic OPTIMIZE."
        ),
    }


UPDATERS = {
    "doris":      bulk_update_doris,
    "duckdb":     bulk_update_duckdb,
    "clickhouse": bulk_update_clickhouse,
}


def main():
    parser = argparse.ArgumentParser(description="W4 — Bulk update benchmark")
    parser.add_argument("--engine", required=True, choices=list(UPDATERS))
    args = parser.parse_args()

    env = {**os.environ}
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                env.setdefault(k.strip(), v.strip())

    print(f"W4 Bulk Update — engine={args.engine}  target={BULK_UPDATE_WHERE}  ({EXPECTED_TOUCH_PCT} rows)")
    t0     = time.perf_counter()
    result = UPDATERS[args.engine](env)
    total  = time.perf_counter() - t0

    record = {
        "workload":  "W4_bulk_update",
        "engine":    args.engine,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "wall_s":    round(total, 3),
        **result,
    }

    print(json.dumps(record, indent=2))
    RESULTS.mkdir(parents=True, exist_ok=True)
    out = RESULTS / f"W4_{args.engine}_{int(time.time())}.jsonl"
    out.write_text(json.dumps(record) + "\n")

    if result.get("status") == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
