# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Layout

All benchmark code lives in `poc/`. The root contains only this file, `README.md`, and `olap_poc_benchmarking_prompt.md` (original requirements). Work exclusively inside `poc/`.

## What This Is

A reproducible performance benchmark comparing **Apache Doris**, **DuckDB**, and **ClickHouse** on a single constrained VM (4 vCPU / 8 GB RAM / 100 GB SSD at `/opt1`). It measures read/write/update performance and GCS integration across 14 read queries and 4 write workloads.

## Environment Setup

```bash
cd poc

# Fill in GCS credentials, engine versions, connection details
cp .env.example .env

# Create venv at the hardcoded location the Makefile expects
python -m venv /opt1/poc/.venv
/opt1/poc/.venv/bin/pip install -r harness/requirements.txt

# VM prep (kernel tunables, Docker install, /opt1 directories) — Rocky Linux 9, requires root
bash scripts/00_vm_prep.sh

# Generate 10M-row Parquet dataset (write/update benchmarks only; GCS read tests use bucket directly)
python data/generate_data.py
```

## Common Commands

All operations are wrapped by `make` (run from `poc/`). `PYTHON` is hardcoded to `/opt1/poc/.venv/bin/python`.

```bash
make help

# One engine at a time
make install-doris / install-duckdb / install-clickhouse
make schema-doris  / schema-duckdb  / schema-clickhouse

# Benchmarks (stop prior engine before starting next)
make bench-doris-local        # local reads + all write workloads
make bench-doris-gcs          # GCS remote reads
make bench-all                # all 3 engines, local only, sequential

# Analyse → report/01_raw_results.csv, 02_summary_table.md, 03_charts/, 04_final_report.md
make analyse

make teardown-all
make clean-results            # delete results/*.jsonl
make status                   # Docker containers, memory, disk
```

### Harness directly

```bash
python harness/run_benchmark.py --engine duckdb --mode local
python harness/run_benchmark.py --engine clickhouse --mode local --queries Q01,Q03,Q05
python harness/run_benchmark.py --engine doris --mode gcs --skip-writes
python harness/run_benchmark.py --engine duckdb --mode local --writes-only
python report/analyse_results.py --results /opt1/poc/results --out report/
```

Runtime env vars that override defaults: `POC_DIR`, `DATA_DIR`, `RESULTS_DIR`, `LOGS_DIR`, `WARM_ITERATIONS`, `QUERY_TIMEOUT_SECONDS`.

## Architecture

### Data Flow

```
data/generate_data.py  →  /opt1/data/*.parquet (hive-partitioned by event_date, seed=42)
                                      ↓
scripts/0{1,2,3}_install_*.sh  →  engine up  →  schema DDL  →  bulk-load local data
                                      ↓
harness/run_benchmark.py  →  results/<engine>_<mode>_<ts>.jsonl
  (--mode gcs reads Parquet directly from GCS bucket; Q14 reads a CSV file, not Parquet)
                                      ↓
report/analyse_results.py  →  report/{01_raw_results.csv, 02_summary_table.md, 03_charts/, 04_final_report.md}
```

### Benchmark Harness (`harness/run_benchmark.py`)

- Per read query: **1 cold run** (drops OS page cache + flushes engine cache) + **5 warm runs**; reports median warm, cold delta, p95
- OOM → recorded as `status: OOM`; never silently retried
- Results written as JSONL (one record per query/workload) to `results/`
- Write workloads (`workloads/W{1..4}_*.py`) are invoked as subprocesses; each prints a JSON line to stdout

### Engine Connectors

| Engine | Protocol | Key env vars |
|--------|----------|-------------|
| Doris | `mysql-connector-python` | `DORIS_HOST`, `DORIS_FE_QUERY_PORT`, `DORIS_USER`, `DORIS_PASSWORD` |
| DuckDB | In-process `duckdb` lib | `DUCKDB_DB_PATH` |
| ClickHouse | HTTP API (`requests`) | `CLICKHOUSE_HOST`, `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` |

### Query / Workload Structure

- `queries/Q{01..14}_<name>/{doris,duckdb,clickhouse}.sql` — 3 SQL dialects per query
- Runtime token replacement: `<GCS_BUCKET>`, `<GCS_PREFIX>`, `<GCS_HMAC_ACCESS_KEY>`, `<GCS_HMAC_SECRET>`
- Q14 (`Q14_gcs_remote_read`) only runs in `--mode gcs`; targets a CSV (`GCS_BUCKET_PREFIX=PC_ITEM_IMAGE.csv`)
- `schema/DIALECT_DIFFERENCES.md` — canonical reference for cross-engine SQL differences

### Docker

Doris (FE + BE) and ClickHouse run via Docker Compose (`docker/`). DuckDB is in-process with no container.

## Known Gotchas

- **ClickHouse SQL files**: strip trailing `--` comments and trailing semicolons before appending `FORMAT JSON` — the harness does this automatically, but new SQL files must not leave bare comments at the end of a statement.
- **ClickHouse schema**: use `clickhouse-client --multiquery` (not HTTP) when running multi-statement DDL (`make schema-clickhouse`).
- **W3 point updates (ClickHouse)**: mutations are async; the harness batch-submits all mutations then polls once at the end — do not add per-row polling.
- **W2 Doris stream-load**: uses NDJSON format (not CSV, not JSON array).
- **Q14 CSV schema**: columns in `queries/Q14_gcs_remote_read/doris.sql` must match the actual CSV layout in the GCS bucket.

## Key Fairness Rules

- Only one engine runs at a time — stop the previous before starting the next
- All engines receive identical Parquet files (deterministic `DATA_SEED=42`)
- SQL semantics are kept equivalent across dialects; differences documented in `schema/DIALECT_DIFFERENCES.md`
- Results with elapsed time < 5 ms are flagged as potential cache hits
