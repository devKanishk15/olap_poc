#!/usr/bin/env python3
"""
run_benchmark.py — Main benchmark harness for the OLAP POC.

Drives all 14 read queries (cold + 5 warm) and all 4 write workloads
for a given engine and mode. Records wall time, row count, peak RSS,
and spill indicators. Writes JSONL to /opt1/olap_poc/poc/results/.

Usage:
    python run_benchmark.py --engine doris      --mode local
    python run_benchmark.py --engine duckdb     --mode gcs
    python run_benchmark.py --engine clickhouse --mode local
    python run_benchmark.py --engine duckdb     --mode local --queries Q01,Q03,Q05
    python run_benchmark.py --engine clickhouse --mode local --skip-writes
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import signal
import resource
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT       = Path(os.environ.get("POC_DIR",     "/opt1/olap_poc/poc"))
DATA_DIR   = Path(os.environ.get("DATA_DIR",    "/opt1/olap_poc/data"))
RESULTS    = Path(os.environ.get("RESULTS_DIR", "/opt1/olap_poc/poc/results"))
QUERIES    = ROOT / "queries"
WORKLOADS  = ROOT / "workloads"
LOGS       = Path(os.environ.get("LOGS_DIR",    "/opt1/olap_poc/logs"))
WARM_ITERS = int(os.environ.get("WARM_ITERATIONS", "5"))
TIMEOUT_S  = int(os.environ.get("QUERY_TIMEOUT_SECONDS", "300"))

QUERY_IDS = [
    "Q01_full_agg",
    "Q02_filtered_agg",
    "Q03_groupby_low_card",
    "Q04_groupby_high_card",
    "Q05_date_range",
    "Q06_topn",
    "Q07_join",
    "Q08_string_like",
    "Q09_approx_distinct",
    "Q10_window_func",
    "Q11_json_extract",
    "Q12_heavy_spill",
    "Q13_multi_dim_groupby",
    "Q14_gcs_remote_read",
]

WRITE_WORKLOADS = ["W1_bulk_load", "W2_micro_batch", "W3_point_update", "W4_bulk_update"]

ENGINES = ["doris", "duckdb", "clickhouse"]
MODES   = ["local", "gcs"]


# ---------------------------------------------------------------------------
# Environment loader
# ---------------------------------------------------------------------------

def load_env() -> dict:
    env = {**os.environ}
    for p in [ROOT / ".env", Path("/opt1/olap_poc/poc/.env")]:
        if p.exists():
            for line in p.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                env.setdefault(k.strip(), v.strip())
    return env


# ---------------------------------------------------------------------------
# Pre-flight schema check
# ---------------------------------------------------------------------------

def _preflight_fail(msg: str) -> None:
    print("FAIL")
    sys.exit(f"\nPreflight check failed: {msg}\n")


def preflight_check(engine: str, env: dict) -> None:
    """Fail fast if the engine isn't reachable or poc.event_fact doesn't exist."""
    print(f"  [preflight] Verifying {engine} connectivity and schema...", end=" ", flush=True)
    try:
        if engine == "doris":
            import mysql.connector
            conn = mysql.connector.connect(
                host=env.get("DORIS_HOST", "127.0.0.1"),
                port=int(env.get("DORIS_FE_QUERY_PORT", "9030")),
                user=env.get("DORIS_USER", "root"),
                password=env.get("DORIS_PASSWORD", ""),
                connection_timeout=15,
            )
            cur = conn.cursor()
            cur.execute("SHOW TABLES FROM poc LIKE 'event_fact'")
            found = cur.fetchall()
            cur.close(); conn.close()
            if not found:
                _preflight_fail(
                    "Table poc.event_fact not found in Doris.\n"
                    "  → Run:  make schema-doris\n"
                    "  → Then load data (if not already done):  make data\n"
                    "  → Then reload into Doris:  "
                    "python harness/run_benchmark.py --engine doris --mode local --writes-only"
                )

        elif engine == "duckdb":
            import duckdb
            con = duckdb.connect(env.get("DUCKDB_DB_PATH", "/opt1/olap_poc/duckdb/benchmark.duckdb"))
            rows = con.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='poc' AND table_name='event_fact'"
            ).fetchall()
            con.close()
            if not rows:
                _preflight_fail(
                    "Table poc.event_fact not found in DuckDB.\n"
                    "  → Run:  make schema-duckdb"
                )

        elif engine == "clickhouse":
            import requests as _req
            host = env.get("CLICKHOUSE_HOST", "127.0.0.1")
            port = env.get("CLICKHOUSE_HTTP_PORT", "8123")
            user = env.get("CLICKHOUSE_USER", "default")
            pw   = env.get("CLICKHOUSE_PASSWORD", "")
            db   = env.get("CLICKHOUSE_DATABASE", "poc")
            resp = _req.get(
                f"http://{host}:{port}/",
                params={"query": (
                    f"SELECT 1 FROM system.tables "
                    f"WHERE database='{db}' AND name='event_fact' FORMAT TSV"
                )},
                auth=(user, pw), timeout=15,
            )
            if resp.status_code != 200 or resp.text.strip() != "1":
                _preflight_fail(
                    f"Table {db}.event_fact not found in ClickHouse.\n"
                    "  → Run:  make schema-clickhouse"
                )

    except SystemExit:
        raise
    except Exception as exc:
        _preflight_fail(
            f"Cannot connect to {engine}: {exc}\n"
            f"  → Is {engine} running?  Try:  make install-{engine}"
        )
    print("OK")


# ---------------------------------------------------------------------------
# Cache flushing
# ---------------------------------------------------------------------------

def drop_os_caches():
    """Drop Linux page cache, dentries, inodes (requires root)."""
    try:
        subprocess.run(["sync"], check=True, timeout=10)
        Path("/proc/sys/vm/drop_caches").write_text("3")
        print("  [cache] OS page cache dropped.")
    except PermissionError:
        print("  [cache] WARNING: Could not drop OS cache (need root). Cold run may be warm.")
    except Exception as exc:
        print(f"  [cache] WARNING: drop_caches failed: {exc}")


def flush_engine_cache(engine: str, env: dict):
    """Send engine-level cache flush instruction."""
    if engine == "clickhouse":
        import requests
        host = env.get("CLICKHOUSE_HOST", "127.0.0.1")
        port = env.get("CLICKHOUSE_HTTP_PORT", "8123")
        user = env.get("CLICKHOUSE_USER", "default")
        pw   = env.get("CLICKHOUSE_PASSWORD", "")
        for sql in ["SYSTEM DROP MARK CACHE", "SYSTEM DROP UNCOMPRESSED CACHE",
                    "SYSTEM DROP DNS CACHE", "SYSTEM RELOAD DICTIONARIES"]:
            try:
                requests.post(f"http://{host}:{port}/", params={"query": sql},
                              auth=(user, pw), timeout=30)
            except Exception:
                pass
        print("  [cache] ClickHouse mark + uncompressed cache flushed.")

    elif engine == "doris":
        # Doris doesn't have a direct cache-drop command; restart BE clears it.
        # As an approximation, run a dummy query and rely on OS cache drop.
        print("  [cache] Doris: relying on OS cache drop (no warm cache flush API).")

    elif engine == "duckdb":
        # DuckDB has no internal cache; OS drop is sufficient.
        print("  [cache] DuckDB: in-process, OS cache drop is sufficient.")


# ---------------------------------------------------------------------------
# Peak RSS measurement
# ---------------------------------------------------------------------------

def get_peak_rss_mb() -> float:
    """Return peak RSS of the current process in MB (Linux /proc/self/status)."""
    try:
        text = Path("/proc/self/status").read_text()
        for line in text.splitlines():
            if line.startswith("VmPeak:"):
                kb = int(line.split()[1])
                return round(kb / 1024, 1)
    except Exception:
        pass
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


# ---------------------------------------------------------------------------
# Per-engine query runners
# ---------------------------------------------------------------------------

def read_sql(query_id: str, engine: str, mode: str, env: dict) -> str:
    """Read SQL file and substitute GCS placeholders if needed."""
    sql_file = QUERIES / query_id / f"{engine}.sql"
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL not found: {sql_file}")
    sql = sql_file.read_text()

    # Substitute GCS placeholders
    sql = sql.replace("<GCS_BUCKET>",          env.get("GCS_BUCKET", "YOUR_BUCKET"))
    sql = sql.replace("<GCS_PREFIX>",          env.get("GCS_BUCKET_PREFIX", "olap_poc/data"))
    sql = sql.replace("${GCS_HMAC_ACCESS_KEY}", env.get("GCS_HMAC_ACCESS_KEY", "KEY"))
    sql = sql.replace("${GCS_HMAC_SECRET}",    env.get("GCS_HMAC_SECRET", "SECRET"))
    sql = sql.replace("<GCS_HMAC_ACCESS_KEY>", env.get("GCS_HMAC_ACCESS_KEY", "KEY"))
    sql = sql.replace("<GCS_HMAC_SECRET>",     env.get("GCS_HMAC_SECRET", "SECRET"))
    return sql


def run_doris(sql: str, env: dict, timeout: int) -> dict:
    import mysql.connector
    host = env.get("DORIS_HOST", "127.0.0.1")
    port = int(env.get("DORIS_FE_QUERY_PORT", "9030"))
    user = env.get("DORIS_USER", "root")
    pw   = env.get("DORIS_PASSWORD", "")

    conn = mysql.connector.connect(
        host=host, port=port, user=user, password=pw,
        database="poc", connection_timeout=timeout
    )
    conn.cmd_query(f"SET query_timeout = {timeout}")
    cur = conn.cursor()

    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    elapsed = time.perf_counter() - t0

    spill = False
    try:
        cur.execute("SHOW PROC '/current_backend_stmts'")
        info = str(cur.fetchall())
        spill = "spill" in info.lower()
    except Exception:
        pass

    cur.close(); conn.close()
    return {"elapsed_s": elapsed, "rows_returned": len(rows), "spill": spill}


def run_duckdb(sql: str, env: dict, timeout: int) -> dict:
    import duckdb
    db_path = env.get("DUCKDB_DB_PATH", "/opt1/olap_poc/duckdb/benchmark.duckdb")
    con = duckdb.connect(db_path)
    con.execute("SET memory_limit = '6GB'")
    con.execute("SET temp_directory = '/opt1/olap_poc/duckdb/spill'")
    con.execute("SET threads = 4")
    con.execute("LOAD httpfs")
    con.execute("LOAD parquet")

    # GCS credentials for remote mode
    con.execute(f"SET s3_endpoint = 'storage.googleapis.com'")
    con.execute(f"SET s3_access_key_id = '{env.get('GCS_HMAC_ACCESS_KEY', '')}'")
    con.execute(f"SET s3_secret_access_key = '{env.get('GCS_HMAC_SECRET', '')}'")
    con.execute(f"SET s3_region = '{env.get('GCS_REGION', 'auto')}'")

    spill_before = sum(f.stat().st_size for f in Path("/opt1/duckdb/spill").rglob("*") if f.is_file())
    t0 = time.perf_counter()
    result = con.execute(sql).fetchall()
    elapsed = time.perf_counter() - t0
    spill_after = sum(f.stat().st_size for f in Path("/opt1/duckdb/spill").rglob("*") if f.is_file())

    con.close()
    return {
        "elapsed_s":    elapsed,
        "rows_returned": len(result),
        "spill":        spill_after > spill_before,
        "spill_bytes":  spill_after - spill_before,
    }


def run_clickhouse(sql: str, env: dict, timeout: int) -> dict:
    import requests
    host   = env.get("CLICKHOUSE_HOST", "127.0.0.1")
    port   = env.get("CLICKHOUSE_HTTP_PORT", "8123")
    user   = env.get("CLICKHOUSE_USER", "default")
    pw     = env.get("CLICKHOUSE_PASSWORD", "")
    db     = env.get("CLICKHOUSE_DATABASE", "poc")
    auth   = (user, pw) if pw else (user, "")
    base   = f"http://{host}:{port}/"

    params = {
        "query":    sql + " FORMAT JSON",
        "database": db,
        "max_execution_time": timeout,
    }
    t0   = time.perf_counter()
    resp = requests.post(base, params=params, auth=auth, timeout=timeout + 30)
    elapsed = time.perf_counter() - t0

    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse error ({resp.status_code}): {resp.text[:500]}")

    data = resp.json()
    rows = len(data.get("data", []))

    # Check spill via system.query_log
    spill = False
    try:
        spill_resp = requests.get(base, params={
            "query": (
                "SELECT written_bytes FROM system.query_log "
                "WHERE type='QueryFinish' ORDER BY event_time DESC LIMIT 1 FORMAT TSV"
            ),
            "database": db,
        }, auth=auth, timeout=10)
        spill_bytes = int(spill_resp.text.strip() or "0")
        spill = spill_bytes > 0
    except Exception:
        pass

    return {"elapsed_s": elapsed, "rows_returned": rows, "spill": spill}


RUNNERS = {
    "doris":      run_doris,
    "duckdb":     run_duckdb,
    "clickhouse": run_clickhouse,
}


# ---------------------------------------------------------------------------
# Single-query benchmark execution
# ---------------------------------------------------------------------------

def run_query(query_id: str, engine: str, mode: str, env: dict) -> Optional[dict]:
    """Run 1 cold + WARM_ITERS warm iterations. Returns aggregated stats or None on skip."""

    # Skip GCS queries in local mode and vice-versa
    is_gcs_query = "gcs" in query_id.lower()
    if mode == "local" and is_gcs_query:
        return {"query_id": query_id, "skipped": True, "reason": "GCS query skipped in local mode"}
    if mode == "gcs" and query_id not in ("Q14_gcs_remote_read",) and not is_gcs_query:
        # In GCS mode, run ALL queries but Q14 uses the GCS table function
        pass

    try:
        sql = read_sql(query_id, engine, mode, env)
    except FileNotFoundError as e:
        return {"query_id": query_id, "skipped": True, "reason": str(e)}

    runner   = RUNNERS[engine]
    timings  = []
    rows_ret = None
    spill    = False
    oom      = False

    print(f"\n  [{query_id}]", end="", flush=True)

    for iteration in range(1 + WARM_ITERS):
        cold = (iteration == 0)
        if cold:
            drop_os_caches()
            flush_engine_cache(engine, env)
            print(" cold", end="", flush=True)
        else:
            print(f" w{iteration}", end="", flush=True)

        rss_before = get_peak_rss_mb()
        try:
            result = runner(sql, env, TIMEOUT_S)
            elapsed    = result["elapsed_s"]
            rows_ret   = result.get("rows_returned", -1)
            spill      = spill or result.get("spill", False)
            rss_after  = get_peak_rss_mb()

            if cold:
                cold_time = elapsed
            else:
                timings.append(elapsed)

            print(f"({elapsed:.2f}s)", end="", flush=True)

            # Flag suspiciously fast results
            if elapsed < 0.005:
                print("[!FAST]", end="", flush=True)

        except MemoryError:
            oom = True
            print("[OOM]", end="", flush=True)
            break
        except Exception as exc:
            err_msg = str(exc)
            if "memory" in err_msg.lower() or "oom" in err_msg.lower():
                oom = True
                print(f"[OOM:{err_msg[:60]}]", end="", flush=True)
                break
            print(f"[ERR:{err_msg[:60]}]", end="", flush=True)
            break

    if oom:
        return {
            "query_id":    query_id,
            "engine":      engine,
            "mode":        mode,
            "status":      "OOM",
        }

    if not timings:
        return {
            "query_id":    query_id,
            "engine":      engine,
            "mode":        mode,
            "status":      "ERROR",
            "cold_s":      cold_time if "cold_time" in dir() else None,
        }

    timings.sort()
    p95_idx = max(int(len(timings) * 0.95) - 1, 0)
    return {
        "query_id":      query_id,
        "engine":        engine,
        "mode":          mode,
        "status":        "OK",
        "cold_s":        round(cold_time, 4),
        "warm_median_s": round(timings[len(timings) // 2], 4),
        "warm_p95_s":    round(timings[p95_idx], 4),
        "warm_min_s":    round(timings[0], 4),
        "warm_max_s":    round(timings[-1], 4),
        "cold_vs_warm":  round(cold_time - timings[len(timings) // 2], 4),
        "rows_returned": rows_ret,
        "spill":         spill,
        "warm_iters":    WARM_ITERS,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="OLAP POC Benchmark Harness")
    parser.add_argument("--engine",      required=True, choices=ENGINES)
    parser.add_argument("--mode",        required=True, choices=MODES)
    parser.add_argument("--queries",     default="ALL",
                        help="Comma-separated query IDs, e.g. Q01,Q03 (default: ALL)")
    parser.add_argument("--skip-writes", action="store_true",
                        help="Skip write workloads (W1–W4)")
    parser.add_argument("--writes-only", action="store_true",
                        help="Run only write workloads, skip reads")
    args = parser.parse_args()

    env = load_env()
    RESULTS.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)

    preflight_check(args.engine, env)

    ts       = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_file = RESULTS / f"{args.engine}_{args.mode}_{ts}.jsonl"

    print("=" * 60)
    print(f"  OLAP POC Benchmark Harness")
    print(f"  Engine : {args.engine}")
    print(f"  Mode   : {args.mode}")
    print(f"  Warm   : {WARM_ITERS} iterations")
    print(f"  Output : {out_file}")
    print("=" * 60)

    all_results = []

    # ---- Read queries ----
    if not args.writes_only:
        if args.queries.upper() == "ALL":
            to_run = QUERY_IDS
        else:
            to_run = [q.strip() for q in args.queries.split(",")]

        # Skip Q14 in local mode
        if args.mode == "local":
            to_run = [q for q in to_run if "gcs_remote_read" not in q.lower()]
        # Only run Q14 in gcs mode alongside others
        print(f"\nRunning {len(to_run)} read queries...")
        for qid in to_run:
            result = run_query(qid, args.engine, args.mode, env)
            if result:
                result["timestamp"] = datetime.now(timezone.utc).isoformat()
                all_results.append(result)
                with open(out_file, "a") as f:
                    f.write(json.dumps(result) + "\n")

    # ---- Write workloads ----
    if not args.skip_writes:
        print(f"\nRunning write workloads...")
        for wname in WRITE_WORKLOADS:
            wscript = WORKLOADS / f"{wname}.py"
            if not wscript.exists():
                print(f"  SKIP: {wname} (script not found)")
                continue
            print(f"\n  [{wname}]", end="", flush=True)
            t0   = time.perf_counter()
            proc = subprocess.run(
                [sys.executable, str(wscript), "--engine", args.engine],
                capture_output=True, text=True, timeout=600,
                env={**os.environ, **env},
            )
            elapsed = time.perf_counter() - t0
            # Parse the JSON result from stdout. Workloads may print with indent=2
            # (multi-line), so join from the first '{'-starting line to end of output.
            stdout_lines = proc.stdout.strip().splitlines()
            json_text = None
            for i, ln in enumerate(stdout_lines):
                if ln.startswith("{"):
                    json_text = "\n".join(stdout_lines[i:])
                    break
            if proc.returncode == 0:
                if json_text:
                    w_result = json.loads(json_text)
                else:
                    w_result = {"status": "OK", "stdout": proc.stdout[-300:]}
            else:
                if json_text:
                    # Workload printed its error record to stdout before exiting non-zero
                    w_result = json.loads(json_text)
                    w_result.setdefault("status", "ERROR")
                else:
                    w_result = {
                        "status": "ERROR",
                        "error":  (proc.stderr or proc.stdout)[-300:],
                    }
            w_result["wall_s"]    = round(elapsed, 3)
            w_result["timestamp"] = datetime.now(timezone.utc).isoformat()
            all_results.append(w_result)
            with open(out_file, "a") as f:
                f.write(json.dumps(w_result) + "\n")
            print(f" {w_result['status']} ({elapsed:.1f}s)")

    # ---- Summary ----
    print("\n" + "=" * 60)
    ok      = sum(1 for r in all_results if r.get("status") == "OK")
    oom     = sum(1 for r in all_results if r.get("status") == "OOM")
    errors  = sum(1 for r in all_results if r.get("status") == "ERROR")
    skipped = sum(1 for r in all_results if r.get("skipped"))
    print(f"  Done: {ok} OK  {oom} OOM  {errors} ERROR  {skipped} SKIPPED")
    print(f"  Results → {out_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
