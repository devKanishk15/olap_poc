# OLAP POC — Full Command Reference

Benchmarks Apache Doris, DuckDB, and ClickHouse on a single VM (4 vCPU / 8 GB RAM / 100 GB SSD at `/opt1`).

---

## 0. Prerequisites

- Rocky Linux 9 (or Ubuntu 22.04) VM
- 4 vCPU, 8 GB RAM, 100 GB SSD mounted at `/opt1`
- Docker + Docker Compose installed (handled by `00_vm_prep.sh`)
- Internet access (Docker Hub, GCS, GitHub releases)
- GCS bucket with Parquet data; Q14 reads `gs://pc_feature/PC_ITEM_IMAGE.csv`

---

## 1. First-Time Setup

### 1a. Clone and configure

```bash
cd /opt1
git clone <repo-url> olap_poc
cd /opt1/olap_poc/poc

# Copy the template and fill in your values
cp .env.example .env
vi .env
```

Key values to fill in `.env`:

| Variable | Description |
|---|---|
| `GCS_BUCKET` | GCS bucket name |
| `GCS_BUCKET_PREFIX` | Key prefix where Parquet files live |
| `GCS_HMAC_ACCESS_KEY` | HMAC key ID |
| `GCS_HMAC_SECRET` | HMAC secret |
| `DORIS_PASSWORD` | Leave blank for default |
| `CLICKHOUSE_PASSWORD` | Leave blank for default |

### 1b. Prepare the VM (kernel tunables, Docker, directories)

```bash
# Run from the poc/ directory — requires root
make prep

# or directly:
sudo bash scripts/00_vm_prep.sh
```

### 1c. Create Python virtual environment and install dependencies

```bash
python3 -m venv /opt1/olap_poc/poc/.venv
/opt1/olap_poc/poc/.venv/bin/pip install --upgrade pip
/opt1/olap_poc/poc/.venv/bin/pip install -r harness/requirements.txt
```

### 1d. Validate the setup

```bash
bash scripts/validate_setup.sh
```

---

## 2. Generate Synthetic Data

> Only needed for **local** mode (write/update/delete benchmarks).
> GCS read benchmarks read directly from the production bucket — no upload needed.

```bash
make data

# or directly:
/opt1/olap_poc/poc/.venv/bin/python data/generate_data.py \
    --rows 10000000 \
    --seed 42 \
    --out  /opt1/olap_poc/data
```

Output: `~30 Parquet partition files` in `/opt1/olap_poc/data/`.

---

## 3. Engine Installation

Install **one engine at a time**. Stop the previous engine before installing the next.

### Apache Doris

```bash
make install-doris

# or directly:
bash scripts/01_install_doris.sh
```

After install, create the schema:

```bash
make schema-doris

# or directly:
mysql -h 127.0.0.1 -P 9030 -u root < schema/doris_ddl.sql
```

### DuckDB

```bash
make install-duckdb

# or directly:
bash scripts/02_install_duckdb.sh
duckdb /opt1/duckdb/benchmark.duckdb < schema/duckdb_ddl.sql
```

### ClickHouse

```bash
make install-clickhouse

# or directly:
bash scripts/03_install_clickhouse.sh
```

After install, create the schema:

```bash
make schema-clickhouse

# or directly:
curl -s "http://127.0.0.1:8123/" --data-binary @schema/clickhouse_ddl.sql
```

---

## 4. Running Benchmarks

### Via Make (recommended)

```bash
# Single engine — local storage
make bench-doris-local
make bench-duckdb-local
make bench-clickhouse-local

# Single engine — GCS remote reads
make bench-doris-gcs
make bench-duckdb-gcs
make bench-clickhouse-gcs

# Full sequential run: all 3 engines, local only (installs, benchmarks, tears down each)
make bench-all
```

### Via harness directly

```bash
# Full run for one engine
/opt1/olap_poc/poc/.venv/bin/python harness/run_benchmark.py --engine doris      --mode local
/opt1/olap_poc/poc/.venv/bin/python harness/run_benchmark.py --engine duckdb     --mode local
/opt1/olap_poc/poc/.venv/bin/python harness/run_benchmark.py --engine clickhouse --mode local

# GCS mode
/opt1/olap_poc/poc/.venv/bin/python harness/run_benchmark.py --engine duckdb --mode gcs

# Run a subset of queries only
/opt1/olap_poc/poc/.venv/bin/python harness/run_benchmark.py --engine clickhouse --mode local \
    --queries Q01,Q03,Q05

# Skip write workloads (reads only)
/opt1/olap_poc/poc/.venv/bin/python harness/run_benchmark.py --engine doris --mode gcs \
    --skip-writes

# Write workloads only (W1–W4, no read queries)
/opt1/olap_poc/poc/.venv/bin/python harness/run_benchmark.py --engine duckdb --mode local \
    --writes-only
```

### Environment variable overrides

```bash
# Override warm iterations and timeout
WARM_ITERATIONS=3 QUERY_TIMEOUT_SECONDS=120 \
    /opt1/olap_poc/poc/.venv/bin/python harness/run_benchmark.py --engine duckdb --mode local

# Override results and log directories
RESULTS_DIR=/tmp/results LOGS_DIR=/tmp/logs \
    /opt1/olap_poc/poc/.venv/bin/python harness/run_benchmark.py --engine clickhouse --mode local
```

---

## 5. Query Reference

| ID | Description | GCS only? |
|----|-------------|-----------|
| Q01 | Full aggregation — COUNT/SUM/AVG over all rows | No |
| Q02 | Filtered aggregation — WHERE clause on low-cardinality column | No |
| Q03 | GROUP BY low-cardinality column | No |
| Q04 | GROUP BY high-cardinality column | No |
| Q05 | Date range filter | No |
| Q06 | TOP-N with ORDER BY + LIMIT | No |
| Q07 | JOIN between fact and dimension | No |
| Q08 | String LIKE / pattern match | No |
| Q09 | Approximate distinct count | No |
| Q10 | Window function (RANK / ROW_NUMBER) | No |
| Q11 | JSON extraction | No |
| Q12 | Heavy spill (large GROUP BY forcing disk spill) | No |
| Q13 | Multi-dimensional GROUP BY | No |
| Q14 | GCS remote read (reads CSV directly from GCS) | **Yes** |

> Q14 is **automatically skipped** in `--mode local`. It only runs in `--mode gcs`.

---

## 6. Write Workload Reference

| ID | Description |
|----|-------------|
| W1 | Bulk load — load all Parquet files in one operation |
| W2 | Micro-batch inserts — 1,000-row batches |
| W3 | Point update — single-row UPDATE by primary key |
| W4 | Bulk update — UPDATE a large fraction of rows |

---

## 7. Analyse Results

```bash
make analyse

# or directly:
/opt1/olap_poc/poc/.venv/bin/python report/analyse_results.py \
    --results /opt1/olap_poc/poc/results \
    --out     report/
```

Output files:

| File | Description |
|------|-------------|
| `report/01_raw_results.csv` | All JSONL results merged into a flat CSV |
| `report/02_summary_table.md` | Markdown table: median warm time per engine/query |
| `report/03_charts/` | PNG bar charts per query group |
| `report/04_final_report.md` | Full narrative report with recommendation |

---

## 8. Health Check

```bash
make status

# or directly:
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
free -h
df -h /opt1
```

---

## 9. Teardown

```bash
# Stop individual engines
make teardown-doris
make teardown-duckdb
make teardown-clickhouse

# Stop everything
make teardown-all

# or directly:
bash scripts/99_teardown.sh --engine doris
bash scripts/99_teardown.sh --engine clickhouse
bash scripts/99_teardown.sh --all

# Delete all JSONL result files (keeps report/ intact)
make clean-results
```

---

## 10. Recommended End-to-End Flow

```bash
# 1. One-time setup
make prep
/opt1/olap_poc/poc/.venv/bin/pip install -r harness/requirements.txt
make data

# 2. Doris
make install-doris
make schema-doris
make bench-doris-local
make bench-doris-gcs
make teardown-doris && sleep 10

# 3. DuckDB
make install-duckdb
make schema-duckdb
make bench-duckdb-local
make bench-duckdb-gcs
make teardown-duckdb && sleep 5

# 4. ClickHouse
make install-clickhouse
make schema-clickhouse
make bench-clickhouse-local
make bench-clickhouse-gcs
make teardown-clickhouse

# 5. Generate report
make analyse
```

Or run everything in one shot (local only):

```bash
make bench-all   # installs → schemas → benchmarks → tears down → analyse
```

---

## 11. Troubleshooting

| Symptom | Fix |
|---------|-----|
| `Preflight check failed: Table poc.event_fact not found` | Run `make schema-<engine>` then load data with `--writes-only` |
| `Cannot connect to doris` | Run `make install-doris`; check `docker ps` |
| `Could not drop OS cache (need root)` | Cold run timing will be unreliable; run as root or with `sudo` |
| DuckDB spill warnings | Expected for Q12; `/opt1/duckdb/spill` must have free space |
| Result files empty | Check `/opt1/olap_poc/poc/results/` — JSONL written per run; check `RESULTS_DIR` override |
| Q14 skipped in local mode | Correct — Q14 is a GCS-only query; use `--mode gcs` to run it |

---

## 12. Key Paths

| Path | Purpose |
|------|---------|
| `/opt1/olap_poc/poc/.env` | Credentials and config (never commit) |
| `/opt1/olap_poc/poc/.venv/` | Python virtual environment |
| `/opt1/olap_poc/data/` | Generated Parquet dataset |
| `/opt1/olap_poc/poc/results/` | JSONL benchmark output files |
| `/opt1/olap_poc/poc/report/` | Analysis outputs (CSV, charts, MD report) |
| `/opt1/olap_poc/logs/` | Engine and harness logs |
| `/opt1/olap_poc/duckdb/spill/` | DuckDB spill-to-disk directory |
