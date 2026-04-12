# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A fully reproducible benchmark POC comparing **Apache Doris**, **DuckDB**, and **ClickHouse** on a single constrained VM (4 vCPU / 8 GB RAM / 100 GB SSD at `/opt1`). It measures read/write performance, memory stability, and GCS integration across 14 read queries and 4 write workloads.

## Environment Setup

```bash
# Copy and fill in credentials (GCS bucket, HMAC key, engine versions)
cp .env.example .env

# Install Python dependencies into the expected venv location
python -m venv /opt1/poc/.venv
/opt1/poc/.venv/bin/pip install -r harness/requirements.txt

# VM prep (kernel tunables, Docker, directories) — requires root on Rocky Linux 9
bash scripts/00_vm_prep.sh

# Generate 10M-row Parquet dataset (local only — used for write/update/delete benchmarks)
python data/generate_data.py
# NOTE: GCS read benchmarks read directly from production data already in the GCS bucket.
# No upload step needed.
```

## Common Commands

All operations are wrapped by `make`. The Makefile hardcodes `PYTHON=/opt1/poc/.venv/bin/python` and reads config from `/opt1/olap_poc/poc/.env`.

```bash
make help                    # List all targets

# Install one engine at a time
make install-doris
make install-duckdb
make install-clickhouse

# Create schema after engine is up
make schema-doris
make schema-duckdb
make schema-clickhouse

# Run benchmarks
make bench-doris-local       # Local storage reads + all write workloads
make bench-doris-gcs         # GCS remote reads
make bench-all               # Full sequential run: all 3 engines, local only

# Analyse results → CSV, charts, summary table, final report
make analyse

# Teardown / cleanup
make teardown-doris
make teardown-all
make clean-results           # Delete *.jsonl from results/
```

### Running the harness directly

```bash
# Full run
python harness/run_benchmark.py --engine duckdb --mode local

# Subset of queries only
python harness/run_benchmark.py --engine clickhouse --mode local --queries Q01,Q03,Q05

# Skip write workloads
python harness/run_benchmark.py --engine doris --mode gcs --skip-writes

# Write workloads only
python harness/run_benchmark.py --engine duckdb --mode local --writes-only

# Analyse results
python report/analyse_results.py --results /opt1/poc/results --out report/
```

## Architecture

### Data flow

```
generate_data.py  →  /opt1/data/*.parquet  (write/update/delete benchmarks only)
                                                       ↓
install_*.sh  →  engine running  →  schema DDL  →  bulk load local data
                                                       ↓
run_benchmark.py  →  results/<engine>_<mode>_<ts>.jsonl
                   (--mode gcs reads directly from production data in GCS bucket)
                                                       ↓
analyse_results.py  →  report/01_raw_results.csv
                     →  report/02_summary_table.md
                     →  report/03_charts/
                     →  report/04_final_report.md
```

### Benchmark harness (`harness/run_benchmark.py`)

- Reads `.env` for credentials and connection details (falls back to hardcoded defaults)
- Per query: **1 cold run** (drops OS page cache + flushes engine cache) + **5 warm runs**
- Reported metric: median of warm runs; cold delta and p95 also recorded
- OOM → recorded as `status: OOM`; never silently retried
- Results written as JSONL (one record per query/workload) to `results/`
- Runtime environment variables override defaults: `POC_DIR`, `DATA_DIR`, `RESULTS_DIR`, `LOGS_DIR`, `WARM_ITERATIONS`, `QUERY_TIMEOUT_SECONDS`

### Engine connectors (in `run_benchmark.py`)

| Engine | Protocol | Key env vars |
|--------|----------|-------------|
| Doris | `mysql-connector-python` | `DORIS_HOST`, `DORIS_FE_QUERY_PORT`, `DORIS_USER`, `DORIS_PASSWORD` |
| DuckDB | In-process `duckdb` Python lib | `DUCKDB_DB_PATH` |
| ClickHouse | HTTP API via `requests` | `CLICKHOUSE_HOST`, `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` |

### Query structure (`queries/`)

14 queries (Q01–Q14), each with three dialect files: `doris.sql`, `duckdb.sql`, `clickhouse.sql`. SQL files use placeholder tokens replaced at runtime:
- `<GCS_BUCKET>`, `<GCS_PREFIX>` — bucket name and key prefix
- `<GCS_HMAC_ACCESS_KEY>`, `<GCS_HMAC_SECRET>` — HMAC credentials

Q14 (`Q14_gcs_remote_read`) only runs in `--mode gcs`; it is skipped automatically in local mode.

**Q14 reads a CSV file, not Parquet.** It targets `gs://pc_feature/PC_ITEM_IMAGE.csv` via the S3-compatible API. Set these values in `.env`:
```
GCS_BUCKET=pc_feature
GCS_BUCKET_PREFIX=PC_ITEM_IMAGE.csv
```
The SQL uses `"format" = "csv"` and `"column_separator" = ","` in the Doris `s3()` TVF. Adjust the `SELECT` columns in `queries/Q14_gcs_remote_read/doris.sql` to match the actual CSV schema.

### Write workloads (`workloads/`)

Four scripts (`W1_bulk_load.py` through `W4_bulk_update.py`) invoked as subprocesses by the harness. Each accepts `--engine <name>` and prints a JSON result line to stdout.

### Schema (`schema/`)

Three DDL files — one per engine. `DIALECT_DIFFERENCES.md` is the canonical reference for cross-engine SQL differences (aggregate functions, NULL handling, JSON extraction, date functions, UPDATE/DELETE semantics, GCS access patterns).

### Docker

Doris (FE + BE) and ClickHouse run via Docker Compose (`docker/`). DuckDB runs as an in-process library with no container.

## Key Fairness Rules

- Only one engine runs at a time — stop the previous before installing the next
- All engines receive identical Parquet files with deterministic seed (`DATA_SEED=42`)
- SQL semantics are kept equivalent across dialects; differences are documented in `schema/DIALECT_DIFFERENCES.md`
- Results with elapsed time < 5 ms are flagged as potential cache hits
