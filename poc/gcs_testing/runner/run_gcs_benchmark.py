#!/usr/bin/env python3
"""
run_gcs_benchmark.py — GCS read-only benchmark runner for glusr_premium_listing.

Runs 10 read queries (GQ01–GQ10) against the glusr_premium_listing CSV file in GCS
directly from each engine's native GCS/S3-compatible table function.
No local data loading is performed.

Usage:
    python run_gcs_benchmark.py --engine doris
    python run_gcs_benchmark.py --engine duckdb --queries GQ01,GQ03
    python run_gcs_benchmark.py --engine clickhouse --dry-run
    python run_gcs_benchmark.py --engine duckdb --timeout 600

Key differences from the parent harness (harness/run_benchmark.py):
    - 1 cold + 3 warm iterations (not 5; 70 GB remote reads are expensive)
    - No OS page cache drop (data is on GCS, not local disk)
    - ClickHouse cold run: drops DNS + mark cache only
    - DuckDB cold run: reopens in-process connection
    - No write workloads, no --mode flag (always GCS)
    - --dry-run: prints substituted SQL for all queries, no engine connection
    - Results go to gcs_testing/results/<engine>_gcs_<timestamp>.jsonl
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
RUNNER_DIR  = Path(__file__).parent
GCS_DIR     = RUNNER_DIR.parent
QUERIES_DIR = GCS_DIR / "queries"
RESULTS_DIR = GCS_DIR / "results"

QUERY_IDS = [
    "GQ01_full_scan_agg",
    "GQ02_filtered_agg",
    "GQ03_groupby_low_card",
    "GQ04_groupby_high_card",
    "GQ05_date_range",
    "GQ06_topn",
    "GQ07_string_like",
    "GQ08_approx_distinct",
    "GQ09_window_func",
    "GQ10_heavy_scan",
]

ENGINES    = ["doris", "duckdb", "clickhouse"]
WARM_ITERS = int(os.environ.get("GCS_WARM_ITERATIONS", "3"))
TIMEOUT_S  = int(os.environ.get("GCS_QUERY_TIMEOUT_SECONDS", "600"))

# DuckDB spill directory — uses same path as parent harness
DUCKDB_SPILL_DIR = Path(os.environ.get("DUCKDB_SPILL_DIR", "/opt1/olap_poc/duckdb/spill"))


# ---------------------------------------------------------------------------
# Environment loader
# ---------------------------------------------------------------------------

def load_env() -> dict:
    """Load .env variables. Search order:
    1. poc/.env  (parent project — same file the main harness uses)
    2. gcs_testing/.env  (local override if present)
    3. OS environment variables (highest priority — already in os.environ)
    """
    env: dict = {}

    # Start from parent poc/.env
    for candidate in [
        GCS_DIR.parent / ".env",   # poc/.env
        GCS_DIR / ".env",          # gcs_testing/.env (optional override)
    ]:
        if candidate.exists():
            for line in candidate.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                env.setdefault(k.strip(), v.strip())

    # OS environment wins over .env file values
    env.update({k: v for k, v in os.environ.items() if k not in env or os.environ.get(k)})

    return env


def validate_env(env: dict) -> None:
    """Raise if required GCS variables are missing."""
    required = ["GCS_BUCKET", "GCS_HMAC_ACCESS_KEY", "GCS_HMAC_SECRET", "GCS_GLUSR_PREMIUM_LISTING_PREFIX"]
    missing  = [k for k in required if not env.get(k) or env[k].startswith("your-")]
    if missing:
        sys.exit(
            "\nERROR: Missing or placeholder GCS credentials in .env:\n"
            + "\n".join(f"  {k}" for k in missing)
            + "\n\nSet these in poc/.env or export them as environment variables.\n"
            "  GCS_GLUSR_PREMIUM_LISTING_PREFIX — path within the bucket, "
            "e.g. pc_feature/GLUSR_PREMIUM_LISTING.csv\n"
        )


# ---------------------------------------------------------------------------
# SQL placeholder substitution
# ---------------------------------------------------------------------------

def substitute_sql(sql: str, env: dict) -> str:
    """Replace GCS placeholder tokens in a SQL file."""
    sql = sql.replace("<GCS_BUCKET>",                          env.get("GCS_BUCKET", "YOUR_BUCKET"))
    sql = sql.replace("<GCS_GLUSR_PREMIUM_LISTING_PREFIX>",    env.get("GCS_GLUSR_PREMIUM_LISTING_PREFIX", "YOUR_PREFIX"))
    sql = sql.replace("<GCS_HMAC_ACCESS_KEY>",                 env.get("GCS_HMAC_ACCESS_KEY", "YOUR_KEY"))
    sql = sql.replace("<GCS_HMAC_SECRET>",                     env.get("GCS_HMAC_SECRET", "YOUR_SECRET"))
    sql = sql.replace("<GCS_REGION>",                          env.get("GCS_REGION", "us-east-1"))
    return sql


def read_sql(query_id: str, engine: str, env: dict) -> str:
    sql_file = QUERIES_DIR / query_id / f"{engine}.sql"
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    return substitute_sql(sql_file.read_text(encoding="utf-8"), env)


# ---------------------------------------------------------------------------
# ClickHouse SQL cleanup
# ---------------------------------------------------------------------------

def _strip_sql(sql: str) -> str:
    """Strip trailing comment lines, whitespace, and semicolons.

    ClickHouse runner appends FORMAT JSON after the real SQL.
    Trailing '--' comment lines or ';' would break that.
    """
    lines = sql.splitlines()
    while lines and lines[-1].strip().startswith("--"):
        lines.pop()
    return "\n".join(lines).rstrip().rstrip(";")


# ---------------------------------------------------------------------------
# Per-engine runners
# ---------------------------------------------------------------------------

def run_doris(sql: str, env: dict, timeout: int) -> dict:
    """Execute SQL via mysql-connector-python (Doris MySQL protocol).

    Connects WITHOUT database= parameter — s3() TVF queries are db-agnostic.
    """
    import mysql.connector

    host = env.get("DORIS_HOST", "127.0.0.1")
    port = int(env.get("DORIS_FE_QUERY_PORT", "9030"))
    user = env.get("DORIS_USER", "root")
    pw   = env.get("DORIS_PASSWORD", "")

    conn = mysql.connector.connect(
        host=host, port=port, user=user, password=pw,
        connection_timeout=timeout,
    )
    conn.cmd_query(f"SET query_timeout = {timeout}")
    cur = conn.cursor()

    t0    = time.perf_counter()
    cur.execute(sql)
    rows  = cur.fetchall()
    elapsed = time.perf_counter() - t0

    cur.close()
    conn.close()
    return {"elapsed_s": elapsed, "rows_returned": len(rows)}


def _duckdb_new_connection(env: dict):
    """Open a fresh DuckDB in-process connection with GCS credentials set."""
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit = '6GB'")
    con.execute(f"SET temp_directory = '{DUCKDB_SPILL_DIR}'")
    con.execute("SET threads = 4")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    # GCS credentials via S3-compatible API
    con.execute("SET s3_endpoint = 'storage.googleapis.com'")
    con.execute(f"SET s3_access_key_id     = '{env.get('GCS_HMAC_ACCESS_KEY', '')}'")
    con.execute(f"SET s3_secret_access_key = '{env.get('GCS_HMAC_SECRET', '')}'")
    con.execute(f"SET s3_region            = '{env.get('GCS_REGION', 'auto')}'")
    con.execute("SET s3_url_style = 'path'")
    return con


# Module-level DuckDB connection (reused across warm runs; re-opened for cold run)
_duckdb_con = None


def run_duckdb(sql: str, env: dict, timeout: int, cold: bool = False) -> dict:
    """Execute SQL in-process via DuckDB.

    cold=True: close existing connection and open a fresh one to clear
    any connection-level state. DuckDB has no persistent in-memory cache
    when using :memory:, but a fresh connection ensures clean state.
    """
    global _duckdb_con

    DUCKDB_SPILL_DIR.mkdir(parents=True, exist_ok=True)

    if cold or _duckdb_con is None:
        if _duckdb_con is not None:
            try:
                _duckdb_con.close()
            except Exception:
                pass
        _duckdb_con = _duckdb_new_connection(env)

    spill_before = _spill_bytes()

    t0     = time.perf_counter()
    result = _duckdb_con.execute(sql).fetchall()
    elapsed = time.perf_counter() - t0

    spill_after = _spill_bytes()
    return {
        "elapsed_s":    elapsed,
        "rows_returned": len(result),
        "spill_bytes":  max(0, spill_after - spill_before),
    }


def _spill_bytes() -> int:
    """Return total bytes in the DuckDB spill directory."""
    try:
        return sum(
            f.stat().st_size
            for f in DUCKDB_SPILL_DIR.rglob("*")
            if f.is_file()
        )
    except Exception:
        return 0


def run_clickhouse(sql: str, env: dict, timeout: int, cold: bool = False) -> dict:
    """Execute SQL via ClickHouse HTTP API.

    cold=True: flush DNS and mark cache before the query.
    """
    import requests

    host = env.get("CLICKHOUSE_HOST", "127.0.0.1")
    port = env.get("CLICKHOUSE_HTTP_PORT", "8123")
    user = env.get("CLICKHOUSE_USER", "default")
    pw   = env.get("CLICKHOUSE_PASSWORD", "")
    auth = (user, pw) if pw else (user, "")
    base = f"http://{host}:{port}/"

    if cold:
        for flush_sql in ["SYSTEM DROP DNS CACHE", "SYSTEM DROP MARK CACHE"]:
            try:
                requests.post(base, params={"query": flush_sql}, auth=auth, timeout=15)
            except Exception:
                pass

    clean_sql = _strip_sql(sql) + " FORMAT JSON"
    params    = {
        "query":            clean_sql,
        "max_execution_time": timeout,
    }

    t0   = time.perf_counter()
    resp = requests.post(base, params=params, auth=auth, timeout=timeout + 30)
    elapsed = time.perf_counter() - t0

    if resp.status_code != 200:
        raise RuntimeError(f"ClickHouse HTTP {resp.status_code}: {resp.text[:500]}")

    data = resp.json()
    rows = len(data.get("data", []))
    return {"elapsed_s": elapsed, "rows_returned": rows}


RUNNERS = {
    "doris":      run_doris,
    "duckdb":     run_duckdb,
    "clickhouse": run_clickhouse,
}


# ---------------------------------------------------------------------------
# OOM detection helpers
# ---------------------------------------------------------------------------

_OOM_KEYWORDS = frozenset([
    "memory", "oom", "out of memory", "memory_limit_exceeded",
    "cannot allocate", "killed",
])


def _is_oom(err: str) -> bool:
    low = err.lower()
    return any(kw in low for kw in _OOM_KEYWORDS)


# ---------------------------------------------------------------------------
# Single-query benchmark
# ---------------------------------------------------------------------------

def run_query(query_id: str, engine: str, env: dict) -> dict:
    """Run 1 cold + WARM_ITERS warm iterations. Return aggregated stats."""
    try:
        sql = read_sql(query_id, engine, env)
    except FileNotFoundError as exc:
        return {
            "query_id": query_id, "engine": engine,
            "status": "SKIP", "error": str(exc),
        }

    runner     = RUNNERS[engine]
    timings:   list[float] = []
    rows_ret   = None
    spill_b    = 0
    oom        = False
    last_err   = None
    cold_time  = None

    print(f"\n  [{query_id}]", end="", flush=True)

    for iteration in range(1 + WARM_ITERS):
        is_cold = (iteration == 0)
        label   = "cold" if is_cold else f"w{iteration}"
        print(f" {label}", end="", flush=True)

        try:
            # Pass cold=True to engines that support it
            if engine in ("duckdb", "clickhouse"):
                result = runner(sql, env, TIMEOUT_S, cold=is_cold)
            else:
                result = runner(sql, env, TIMEOUT_S)

            elapsed  = result["elapsed_s"]
            rows_ret = result.get("rows_returned", -1)
            spill_b  = max(spill_b, result.get("spill_bytes", 0))

            if is_cold:
                cold_time = elapsed
            else:
                timings.append(elapsed)

            print(f"({elapsed:.2f}s)", end="", flush=True)

            if elapsed < 0.005:
                print("[!FAST]", end="", flush=True)

        except MemoryError:
            oom = True
            last_err = "MemoryError"
            print("[OOM]", end="", flush=True)
            break
        except Exception as exc:
            err_msg = str(exc)
            if _is_oom(err_msg):
                oom = True
                last_err = err_msg
                print(f"[OOM:{err_msg[:60]}]", end="", flush=True)
                break
            last_err = err_msg
            print(f"[ERR:{err_msg[:60]}]", end="", flush=True)
            break

    if oom:
        return {
            "query_id":    query_id,
            "engine":      engine,
            "status":      "OOM",
            "cold_s":      cold_time,
            "warm_median_s": None,
            "warm_min_s":  None,
            "warm_max_s":  None,
            "rows_returned": rows_ret,
            "oom":         True,
            "error":       last_err,
            "warm_iters":  0,
            "spill_bytes": spill_b,
            "gcs_prefix":  env.get("GCS_GLUSR_PREMIUM_LISTING_PREFIX", ""),
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        }

    if not timings:
        return {
            "query_id":    query_id,
            "engine":      engine,
            "status":      "ERROR",
            "cold_s":      cold_time,
            "warm_median_s": None,
            "warm_min_s":  None,
            "warm_max_s":  None,
            "rows_returned": rows_ret,
            "oom":         False,
            "error":       last_err,
            "warm_iters":  0,
            "spill_bytes": spill_b,
            "gcs_prefix":  env.get("GCS_GLUSR_PREMIUM_LISTING_PREFIX", ""),
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        }

    timings.sort()
    mid = len(timings) // 2
    return {
        "query_id":      query_id,
        "engine":        engine,
        "status":        "OK",
        "cold_s":        round(cold_time, 4) if cold_time is not None else None,
        "warm_median_s": round(timings[mid], 4),
        "warm_min_s":    round(timings[0], 4),
        "warm_max_s":    round(timings[-1], 4),
        "rows_returned": rows_ret,
        "oom":           False,
        "error":         None,
        "warm_iters":    len(timings),
        "spill_bytes":   spill_b,
        "gcs_prefix":    env.get("GCS_GLUSR_PREMIUM_LISTING_PREFIX", ""),
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Dry-run mode
# ---------------------------------------------------------------------------

def dry_run(engine: str, query_ids: list[str], env: dict) -> None:
    """Print resolved SQL for all queries without executing anything."""
    # Print the preamble the runner would inject for DuckDB
    if engine == "duckdb":
        print("\n=== DuckDB session preamble (injected by runner) ===")
        print("LOAD httpfs;")
        print("SET s3_endpoint = 'storage.googleapis.com';")
        print(f"SET s3_access_key_id     = '{env.get('GCS_HMAC_ACCESS_KEY', '<KEY>')[:6]}...';")
        print(f"SET s3_secret_access_key = '***';")
        print(f"SET s3_region            = '{env.get('GCS_REGION', 'auto')}';")
        print("SET memory_limit = '6GB';")
        print(f"SET temp_directory = '{DUCKDB_SPILL_DIR}';")

    for qid in query_ids:
        print(f"\n{'='*70}")
        print(f"=== DRY RUN: {qid} / {engine} ===")
        print("="*70)
        try:
            sql = read_sql(qid, engine, env)
            if engine == "clickhouse":
                sql = _strip_sql(sql) + " FORMAT JSON"
            print(sql)
        except FileNotFoundError as exc:
            print(f"[SKIP — {exc}]")

    print(f"\n{'='*70}")
    print(f"Dry run complete. {len(query_ids)} queries shown for engine '{engine}'.")
    print("No engine connections were made.")


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

def print_summary(results: list[dict]) -> None:
    try:
        from tabulate import tabulate
    except ImportError:
        print("\n[summary] Install 'tabulate' for a formatted table: pip install tabulate")
        for r in results:
            status = r.get("status", "?")
            cold   = f"{r['cold_s']:.2f}s" if r.get("cold_s") is not None else "--"
            warm   = f"{r['warm_median_s']:.2f}s" if r.get("warm_median_s") is not None else "--"
            print(f"  {r['query_id']:<35} {status:<8} cold={cold:<10} warm={warm}")
        return

    rows = []
    for r in results:
        cold = f"{r['cold_s']:.2f}" if r.get("cold_s") is not None else "--"
        warm = f"{r['warm_median_s']:.2f}" if r.get("warm_median_s") is not None else "--"
        spill = f"{r.get('spill_bytes', 0) // (1024*1024)}MB" if r.get("spill_bytes") else "-"
        rows.append([
            r["query_id"],
            r.get("status", "?"),
            cold,
            warm,
            r.get("rows_returned", "--"),
            spill,
        ])

    print("\n" + "="*70)
    print(tabulate(
        rows,
        headers=["Query", "Status", "cold_s", "warm_median_s", "rows", "spill"],
        tablefmt="grid",
    ))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    global WARM_ITERS, TIMEOUT_S
    parser = argparse.ArgumentParser(
        description="GCS read-only benchmark runner for glusr_premium_listing CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_gcs_benchmark.py --engine duckdb
  python run_gcs_benchmark.py --engine clickhouse --queries GQ01,GQ03,GQ08
  python run_gcs_benchmark.py --engine doris --dry-run
  python run_gcs_benchmark.py --engine duckdb --timeout 900
        """,
    )
    parser.add_argument("--engine",   required=True, choices=ENGINES,
                        help="Engine to benchmark")
    parser.add_argument("--queries",  default="ALL",
                        help="Comma-separated GQ IDs, e.g. GQ01,GQ03 (default: ALL)")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Print resolved SQL without executing. No engine needed.")
    parser.add_argument("--timeout",  type=int, default=TIMEOUT_S,
                        help=f"Per-query timeout in seconds (default: {TIMEOUT_S})")
    parser.add_argument("--warm-iters", type=int, default=WARM_ITERS,
                        help=f"Number of warm iterations (default: {WARM_ITERS})")
    args = parser.parse_args()

    # Override globals from CLI args
    WARM_ITERS = args.warm_iters
    TIMEOUT_S  = args.timeout

    env = load_env()

    # Resolve query list
    if args.queries.upper() == "ALL":
        to_run = list(QUERY_IDS)
    else:
        resolved = []
        unknown = []
        for q in [x.strip() for x in args.queries.split(",")]:
            if q in QUERY_IDS:
                resolved.append(q)
            else:
                matches = [qid for qid in QUERY_IDS if qid.startswith(q)]
                if matches:
                    resolved.extend(matches)
                else:
                    unknown.append(q)
        if unknown:
            sys.exit(
                f"\nERROR: Unknown query ID(s): {', '.join(unknown)}\n"
                f"Valid IDs: {', '.join(QUERY_IDS)}\n"
            )
        to_run = resolved

    # Dry-run: no engine validation needed
    if args.dry_run:
        dry_run(args.engine, to_run, env)
        return

    # Live run: validate credentials
    validate_env(env)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts       = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_file = RESULTS_DIR / f"{args.engine}_gcs_{ts}.jsonl"

    print("=" * 70)
    print(f"  GCS Read Benchmark — glusr_premium_listing")
    print(f"  Engine  : {args.engine}")
    print(f"  Queries : {len(to_run)}")
    print(f"  Warm    : {WARM_ITERS} iterations per query")
    print(f"  Timeout : {TIMEOUT_S}s per query")
    print(f"  Prefix  : {env.get('GCS_GLUSR_PREMIUM_LISTING_PREFIX', '?')}")
    print(f"  Output  : {out_file}")
    print("=" * 70)

    all_results: list[dict] = []

    for qid in to_run:
        result = run_query(qid, args.engine, env)
        all_results.append(result)
        with open(out_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(result) + "\n")

    # Close DuckDB connection at end
    global _duckdb_con
    if _duckdb_con is not None:
        try:
            _duckdb_con.close()
        except Exception:
            pass

    print_summary(all_results)

    ok      = sum(1 for r in all_results if r.get("status") == "OK")
    oom     = sum(1 for r in all_results if r.get("status") == "OOM")
    errors  = sum(1 for r in all_results if r.get("status") == "ERROR")
    skipped = sum(1 for r in all_results if r.get("status") == "SKIP")

    print(f"\n  Done: {ok} OK  {oom} OOM  {errors} ERROR  {skipped} SKIP")
    print(f"  Results → {out_file}")
    print("=" * 70)


if __name__ == "__main__":
    main()
