# Trino 481 Benchmarking Guide

Step-by-step commands to install, configure, and benchmark Trino 481 on the OLAP POC VM.

---

## Prerequisites

### 1. VM and base setup (if not already done)

```bash
cd /opt1/olap_poc/poc

# VM kernel tunables, Docker install, /opt1 directory tree
sudo bash scripts/00_vm_prep.sh

# Python venv (required by the harness)
python3 -m venv /opt1/poc/.venv
/opt1/poc/.venv/bin/pip install -r harness/requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in the required values:

```ini
# GCS credentials (needed for GCS benchmark mode)
GCS_BUCKET=your-gcs-bucket-name
GCS_BUCKET_PREFIX=olap_poc/data
GCS_HMAC_ACCESS_KEY=GOOGXXXXXXXXXXXXXXXX
GCS_HMAC_SECRET=your-hmac-secret-here
GCS_S3_ENDPOINT=https://storage.googleapis.com
GCS_REGION=auto

# Trino connection (defaults work — change only if port conflicts)
TRINO_VERSION=481
TRINO_IMAGE=trinodb/trino:481
TRINO_HOST=127.0.0.1
TRINO_PORT=8080
TRINO_USER=trino

# GCS listing table prefix (used by gcs_testing queries)
GCS_GLUSR_PREMIUM_LISTING_PREFIX=pc_feature/GLUSR_PREMIUM_LISTING.csv
```

> **Note:** Stop any other running engine before starting Trino.
> Each engine should run alone to ensure fair resource allocation.

---

## Step 1 — Tear down any running engine

```bash
# Stop all other engines first (skip if none are running)
make teardown-doris
make teardown-duckdb
make teardown-clickhouse

# Or stop everything at once
make teardown-all
```

---

## Step 2 — Install and start Trino

```bash
make install-trino
```

This single command:
- Creates required directories under `/opt1/olap_poc/trino/`
- Copies Trino config files (`config.properties`, `jvm.config`, `node.properties`)
- Substitutes GCS credentials into the `gcs_hive` catalog config
- Starts the `trinodb/trino:481` Docker container (7 GB RAM limit, 4 CPUs)
- Polls `http://localhost:8080/v1/info` until Trino is ready (up to 120 s)

To verify Trino is running:

```bash
# Check container status
docker ps --filter name=trino

# Check Trino health endpoint
curl -s http://localhost:8080/v1/info | python3 -m json.tool

# Check logs if something looks wrong
docker logs trino --tail 50
```

---

## Step 3 — Create schemas

### 3a. Local schema (reads `/opt1/olap_poc/data/`)

```bash
make schema-trino-local
```

This runs `schema/trino_local_ddl.sql` which:
- Creates schema `poc` in the `local_hive` catalog
- Creates external Hive table `poc.event_fact` partitioned by `event_date`
- Calls `CALL system.sync_partition_metadata(...)` to register all 30 date partitions

Verify:

```bash
docker exec -i trino trino \
  --server http://localhost:8080 \
  --catalog local_hive \
  --schema poc \
  --execute "SELECT COUNT(*) FROM poc.event_fact;"
```

Expected output: `10000000`

### 3b. GCS schema (reads from GCS bucket)

```bash
make schema-trino-gcs
```

This runs `schema/trino_gcs_ddl.sql` (with GCS bucket/prefix substituted) which:
- Creates schema `poc` in the `gcs_hive` catalog
- Creates external Hive table `poc.event_fact` pointing at GCS Parquet files
- Creates external Hive table `poc.glusr_premium_listing` pointing at the GCS CSV file

Verify:

```bash
docker exec -i trino trino \
  --server http://localhost:8080 \
  --catalog gcs_hive \
  --schema poc \
  --execute "SELECT COUNT(*) FROM poc.glusr_premium_listing;"
```

---

## Step 4 — Run benchmarks

### 4a. Local benchmark (Q01–Q13 + write workloads W1, W2)

```bash
make bench-trino-local
```

Or run the harness directly:

```bash
/opt1/poc/.venv/bin/python harness/run_benchmark.py \
  --engine trino \
  --mode local
```

**What runs:**
| Workload | Description | Expected result |
|----------|-------------|-----------------|
| Q01–Q13  | 13 read queries (aggregation, joins, window funcs, JSON, approx distinct) | Timed results |
| Q14      | Skipped in local mode (GCS-only query) | — |
| W1       | CTAS bulk load into managed ORC table | Rows/s metric |
| W2       | VALUES-based micro-batch INSERT | Rows/s metric |
| W3       | Point update | `FEATURE_GAP` (Hive external is immutable) |
| W4       | Bulk update | `FEATURE_GAP` (Hive external is immutable) |

### 4b. GCS benchmark (Q14 + GQ01–GQ10)

```bash
make bench-trino-gcs
```

Or run the harness directly:

```bash
/opt1/poc/.venv/bin/python harness/run_benchmark.py \
  --engine trino \
  --mode gcs
```

**What runs:**
| Workload | Description |
|----------|-------------|
| Q01–Q14  | All 14 read queries via `gcs_hive` catalog |
| GQ01–GQ10 | 10 dedicated GCS queries on `glusr_premium_listing` |

### Run specific queries only

```bash
# Local — only Q01, Q03, Q09
/opt1/poc/.venv/bin/python harness/run_benchmark.py \
  --engine trino --mode local \
  --queries Q01,Q03,Q09

# GCS — skip write workloads
/opt1/poc/.venv/bin/python harness/run_benchmark.py \
  --engine trino --mode gcs \
  --skip-writes

# Local — write workloads only
/opt1/poc/.venv/bin/python harness/run_benchmark.py \
  --engine trino --mode local \
  --writes-only
```

---

## Step 5 — Monitor during benchmarks

```bash
# Watch Trino resource usage in real time
watch -n2 'docker stats trino --no-stream'

# Tail the harness log
tail -f /opt1/olap_poc/logs/trino_*.log

# Check available RAM
free -h

# Check disk usage
df -h /opt1
```

---

## Step 6 — Analyse results

```bash
make analyse
```

Or directly:

```bash
/opt1/poc/.venv/bin/python report/analyse_results.py \
  --results /opt1/poc/results \
  --out report/
```

Output files:

| File | Contents |
|------|----------|
| `report/01_raw_results.csv` | All raw timing records |
| `report/02_summary_table.md` | Median warm time comparison across all engines |
| `report/03_charts/` | PNG bar charts per query |
| `report/04_final_report.md` | Executive summary with rankings |

---

## Step 7 — Teardown

```bash
make teardown-trino
```

To also wipe Trino data and metastore directories:

```bash
bash scripts/99_teardown.sh --engine trino --wipe
```

---

## Full sequential run (all 4 engines)

To benchmark all engines in sequence (local mode only):

```bash
make bench-all
```

This runs: Doris → DuckDB → ClickHouse → Trino, each isolated, then calls `make analyse`.

---

## Troubleshooting

### Trino container won't start

```bash
docker logs trino 2>&1 | tail -100
```

Common causes:
- Port 8080 already in use → check `lsof -i :8080` and kill the occupying process
- Insufficient memory → ensure < 1 GB is used by other processes (`free -h`)
- Config file permission issue → `sudo chown -R 1000:1000 /opt1/olap_poc/trino/`

### Schema creation fails

```bash
# Confirm container is reachable
curl -s http://localhost:8080/v1/info

# Re-run DDL manually
docker exec -i trino trino \
  --server http://localhost:8080 \
  --catalog local_hive \
  --schema poc \
  < schema/trino_local_ddl.sql
```

### Partition sync returns 0 rows

```bash
# Force re-sync partitions
docker exec -i trino trino \
  --server http://localhost:8080 \
  --catalog local_hive \
  --schema poc \
  --execute "CALL system.sync_partition_metadata('poc', 'event_fact', 'FULL');"

# Verify partition count
docker exec -i trino trino \
  --server http://localhost:8080 \
  --catalog local_hive \
  --schema poc \
  --execute "SELECT COUNT(*) FROM \"$partitions\".poc.event_fact;"
```

### GCS queries fail with auth error

```bash
# Check GCS credentials are substituted in catalog file
grep -E "access-key|secret-key|endpoint" \
  /opt1/olap_poc/trino/etc/catalog/gcs_hive.properties

# Re-run GCS credential substitution manually
GCS_HMAC_ACCESS_KEY=$(grep GCS_HMAC_ACCESS_KEY .env | cut -d= -f2)
GCS_HMAC_SECRET=$(grep GCS_HMAC_SECRET .env | cut -d= -f2)
GCS_S3_ENDPOINT=$(grep GCS_S3_ENDPOINT .env | cut -d= -f2)

sed -i \
  -e "s|\${ENV:GCS_HMAC_ACCESS_KEY}|${GCS_HMAC_ACCESS_KEY}|g" \
  -e "s|\${ENV:GCS_HMAC_SECRET}|${GCS_HMAC_SECRET}|g" \
  -e "s|\${ENV:GCS_S3_ENDPOINT}|${GCS_S3_ENDPOINT}|g" \
  /opt1/olap_poc/trino/etc/catalog/gcs_hive.properties

# Restart container to reload catalog
docker restart trino
sleep 20
```

### Query timeout

Increase the timeout (default 300 s):

```bash
QUERY_TIMEOUT_SECONDS=600 /opt1/poc/.venv/bin/python \
  harness/run_benchmark.py --engine trino --mode local
```

Or set it permanently in `.env`:

```ini
QUERY_TIMEOUT_SECONDS=600
```

---

## Quick reference

```bash
make install-trino          # Install + start Trino
make schema-trino-local     # Create local Hive tables
make schema-trino-gcs       # Create GCS Hive tables
make bench-trino-local      # Run local benchmarks
make bench-trino-gcs        # Run GCS benchmarks
make teardown-trino         # Stop Trino
make analyse                # Generate report
make status                 # Check containers + memory + disk
```
