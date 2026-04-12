# OLAP Engine POC — Apache Doris vs DuckDB vs ClickHouse

## Overview

This repository contains a fully reproducible Proof of Concept (POC) that benchmarks three OLAP engines—**Apache Doris**, **DuckDB**, and **ClickHouse**—on a single constrained VM (4 vCPU / 8 GB RAM / 100 GB SSD).

The goal is to produce defensible performance numbers and a clear engineering recommendation covering:

- **Read performance** from GCS (external/remote) and local SSD
- **Write performance** — bulk load, micro-batch inserts, point updates, bulk updates
- **Memory stability** — OOM detection, spill-to-disk tracking
- **Qualitative factors** — operability, ecosystem, GCS integration maturity

---

## Directory Structure

```
poc/
├── README.md                          ← You are here
├── .env.example                       ← Template for credentials & versions
├── scripts/
│   ├── 00_vm_prep.sh                  ← System prerequisites & tuning
│   ├── 01_install_doris.sh            ← Doris install & config
│   ← 02_install_duckdb.sh            ← DuckDB install & config
│   ├── 03_install_clickhouse.sh       ← ClickHouse install & config
│   └── 99_teardown.sh                 ← Full cleanup
├── docker/
│   ├── doris-compose.yml
│   └── clickhouse-compose.yml
├── schema/
│   ├── table_spec.md                  ← Canonical 50+ column design doc
│   ├── doris_ddl.sql
│   ├── duckdb_ddl.sql
│   └── clickhouse_ddl.sql
├── data/
│   └── generate_data.py               ← Synthetic 10M-row Parquet generator (local only, for write benchmarks)
├── queries/
│   ├── Q01_full_agg/
│   │   ├── doris.sql
│   │   ├── duckdb.sql
│   │   └── clickhouse.sql
│   └── ... (Q02 through Q14)
├── workloads/
│   ├── W1_bulk_load.py
│   ├── W2_micro_batch.py
│   ├── W3_point_update.py
│   └── W4_bulk_update.py
├── harness/
│   ├── run_benchmark.py               ← Main benchmark runner
│   └── requirements.txt
├── results/                           ← JSONL outputs land here at runtime
└── report/
    ├── 01_raw_results.csv
    ├── 02_summary_table.md
    ├── 03_charts/
    └── 04_final_report.md
```

---

## Prerequisites

- Ubuntu 22.04 LTS VM
- 4 vCPU, 8 GB RAM, 100 GB SSD mounted at `/opt1`
- Internet access (Docker Hub, GCS)
- GCS bucket with data files (Parquet/CSV/ORC)

---

## Quick-Start (reproducing the full POC)

```bash
# 1. Clone / copy this repo to your VM
cd /opt1 && git clone <repo-url> poc

# 2. Configure credentials
cp .env.example .env
vi .env   # fill in GCS_BUCKET, HMAC_KEY, engine versions, etc.

# 3. Prepare the VM (kernel tunables, Docker, directories)
bash scripts/00_vm_prep.sh

# 4. Generate synthetic data (local only — used for write/update/delete benchmarks)
cd data
pip install -r ../harness/requirements.txt
python generate_data.py        # writes /opt1/data/
# NOTE: GCS read benchmarks (--mode gcs) read directly from production data
#       already present in the GCS bucket. No upload step needed.

# 5. Install & benchmark each engine (one at a time)
#    Doris
bash scripts/01_install_doris.sh
python harness/run_benchmark.py --engine doris --mode local
python harness/run_benchmark.py --engine doris --mode gcs

#    Stop Doris, start DuckDB
bash scripts/99_teardown.sh --engine doris
bash scripts/02_install_duckdb.sh
python harness/run_benchmark.py --engine duckdb --mode local
python harness/run_benchmark.py --engine duckdb --mode gcs

#    Stop DuckDB, start ClickHouse
bash scripts/99_teardown.sh --engine duckdb
bash scripts/03_install_clickhouse.sh
python harness/run_benchmark.py --engine clickhouse --mode local
python harness/run_benchmark.py --engine clickhouse --mode gcs

# 6. Analyse results
python report/analyse_results.py
```

---

## Benchmarking Methodology

### Cold vs Warm Runs
- **1 cold run**: OS page cache dropped (`echo 3 > /proc/sys/vm/drop_caches`) + engine-level cache flush before execution.
- **5 warm runs**: Back-to-back without cache flush.
- **Reported metric**: Median of the 5 warm runs as headline; p95 alongside; cold delta noted.

### Fairness Rules
1. Only one engine running at a time — others stopped.
2. All engines receive identical data (same Parquet files, same row count, deterministic seed).
3. SQL semantics are kept identical across dialects; any differences are explicitly documented.
4. A single run is **never** used as the headline number.
5. OOM → query marked `OOM`, not silently retried.
6. Suspiciously fast results (< 5 ms) are flagged as potential cache hits.

### Engine Versions (pin in `.env`)
| Engine | Target Version | Notes |
|--------|---------------|-------|
| Apache Doris | 2.1.x | Latest stable LTS |
| DuckDB | 1.x | Current stable |
| ClickHouse | 24.x | Current stable |

---

## GCS Authentication

See `.env.example` for variable names. Two supported methods:

| Method | Variable | Used by |
|--------|----------|---------|
| HMAC Key | `GCS_HMAC_ACCESS_KEY` + `GCS_HMAC_SECRET` | Doris TVF, ClickHouse `s3()`, DuckDB httpfs |
| Service Account JSON | `GCS_SA_JSON_PATH` | DuckDB `gcs` extension (if preferred) |

---

## Contact / Author

Benchmarking POC — generated as part of OLAP engine evaluation.
