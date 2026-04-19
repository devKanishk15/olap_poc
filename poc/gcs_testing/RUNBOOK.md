# GCS Read Benchmark Runbook

Step-by-step guide to run the `glusr_premium_listing` GCS read benchmarks across
Apache Doris, DuckDB, and ClickHouse.

All commands assume you are on the benchmark VM (`/opt1` mount) and that
`poc/` is the working directory unless stated otherwise.

---

## Prerequisites Checklist

| Item | How to verify |
|---|---|
| Rocky Linux 9 VM, 4 vCPU / 8 GB RAM / 100 GB SSD at `/opt1` | `nproc && free -h && df -h /opt1` |
| Docker + Docker Compose installed | `docker --version && docker compose version` |
| Python 3.10+ | `python3 --version` |
| `poc/.env` exists and is populated | `ls -la /opt1/olap_poc/poc/.env` |
| GCS HMAC key with read access on the `pc_feature` bucket | `gsutil ls gs://pc_feature/` |
| No other engine running (avoid RAM contention) | `docker ps` — should show no running containers before each engine run |

---

## Step 1 — Configure `.env`

The runner reads `poc/.env` (the same file used by the parent harness).
One new variable — `GCS_GLUSR_PREMIUM_LISTING_PREFIX` — must be added.

```bash
vi /opt1/olap_poc/poc/.env
```

Ensure the following are set:

```bash
# ── GCS credentials ──────────────────────────────────────────────────────────
GCS_BUCKET=pc_feature
GCS_HMAC_ACCESS_KEY=GOOGxxxxxxxxxxxxxx
GCS_HMAC_SECRET=your-hmac-secret
GCS_REGION=auto

# ── NEW: path to glusr_premium_listing CSV within the bucket ─────────────────
GCS_GLUSR_PREMIUM_LISTING_PREFIX=pc_feature/GLUSR_PREMIUM_LISTING.csv

# ── Engine connection defaults (change only if ports differ) ──────────────────
DORIS_HOST=127.0.0.1
DORIS_FE_QUERY_PORT=9030
DORIS_USER=root
DORIS_PASSWORD=

CLICKHOUSE_HOST=127.0.0.1
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
```

> **Note:** `GCS_GLUSR_PREMIUM_LISTING_PREFIX` includes the bucket name as its
> first path component (e.g. `pc_feature/GLUSR_PREMIUM_LISTING.csv`) so that
> DuckDB can use it directly as `s3://<prefix>`. Doris uses
> `s3://<GCS_BUCKET>/<prefix>` and ClickHouse uses
> `https://storage.googleapis.com/<GCS_BUCKET>/<prefix>`.

Verify GCS access before proceeding:

```bash
gsutil ls gs://pc_feature/GLUSR_PREMIUM_LISTING.csv
```

---

## Step 2 — Install Python Dependencies

```bash
cd /opt1/olap_poc/poc

# Preferred: reuse the existing parent harness venv
/opt1/olap_poc/poc/.venv/bin/pip install -q tabulate rich

# Alternative: standalone venv for gcs_testing only
python3 -m venv /opt1/olap_poc/poc/.gcs_venv
/opt1/olap_poc/poc/.gcs_venv/bin/pip install -r gcs_testing/runner/requirements.txt
```

---

## Step 3 — Dry Run (No Engine Required)

Always run `--dry-run` first. It substitutes all placeholders and prints the
resolved SQL without connecting to any engine. Use this to catch
misconfigured credentials or wrong bucket paths before paying for a
full network scan.

```bash
cd /opt1/olap_poc/poc

# DuckDB — also prints the session preamble the runner injects
python3 gcs_testing/runner/run_gcs_benchmark.py --engine duckdb --dry-run

# ClickHouse — verify: each query ends with FORMAT JSON, no trailing semicolons
python3 gcs_testing/runner/run_gcs_benchmark.py --engine clickhouse --dry-run

# Doris
python3 gcs_testing/runner/run_gcs_benchmark.py --engine doris --dry-run
```

**What to verify in the output:**

- All four placeholders are replaced with real values — none of
  `<GCS_BUCKET>`, `<GCS_HMAC_ACCESS_KEY>`, `<GCS_HMAC_SECRET>`,
  `<GCS_GLUSR_PREMIUM_LISTING_PREFIX>` appear literally in the printed SQL.
- Doris / DuckDB URI looks like `s3://pc_feature/GLUSR_PREMIUM_LISTING.csv`
- ClickHouse URI looks like
  `https://storage.googleapis.com/pc_feature/GLUSR_PREMIUM_LISTING.csv`
- ClickHouse queries do **not** end with `;`
- ClickHouse queries do **not** end with a bare `--` comment line

---

## Step 4 — Smoke Test (Single Query)

Before running all 10 queries, validate connectivity end-to-end with one
cheap query. GQ01 (full scan + aggregate) is the simplest — it reads the
entire file but returns only one row.

```bash
cd /opt1/olap_poc/poc

# Run only GQ01 against whichever engine is currently running
python3 gcs_testing/runner/run_gcs_benchmark.py --engine duckdb --queries GQ01
```

Expected output:

```
======================================================================
  GCS Read Benchmark — glusr_premium_listing
  Engine  : duckdb
  Queries : 1
  Warm    : 3 iterations per query
  Timeout : 600s per query
  Prefix  : pc_feature/GLUSR_PREMIUM_LISTING.csv
  Output  : gcs_testing/results/duckdb_gcs_20260420T102300Z.jsonl
======================================================================

  [GQ01_full_scan_agg] cold cold(42.38s) w1(38.44s) w2(37.22s) w3(40.00s)
  ...
```

If you see `rows_returned = 0`, the CSV path is wrong — recheck
`GCS_GLUSR_PREMIUM_LISTING_PREFIX`.

---

## Step 5 — Full Benchmark Run per Engine

Run **one engine at a time**. Stop all other containers before starting
each engine to avoid RAM contention on the 8 GB VM.

---

### 5A — DuckDB

DuckDB runs in-process — no Docker container required.

```bash
# 1. Stop any running containers
docker stop $(docker ps -q) 2>/dev/null
sleep 5

# 2. Verify RAM is available (expect > 5 GB free)
free -h

# 3. Run all 10 GCS queries
cd /opt1/olap_poc/poc
python3 gcs_testing/runner/run_gcs_benchmark.py --engine duckdb

# 4. Confirm result file was written
ls -lh gcs_testing/results/duckdb_gcs_*.jsonl
```

---

### 5B — ClickHouse

```bash
# 1. Stop any running containers
docker stop $(docker ps -q) 2>/dev/null
sleep 10

# 2. Start ClickHouse
cd /opt1/olap_poc/poc
docker compose -f docker/clickhouse-compose.yml up -d

# 3. Wait for startup (~20 s) and verify
sleep 20
curl -s "http://127.0.0.1:8123/ping"
# Expected response: Ok.

# 4. Run all 10 GCS queries
python3 gcs_testing/runner/run_gcs_benchmark.py --engine clickhouse

# 5. Confirm result file
ls -lh gcs_testing/results/clickhouse_gcs_*.jsonl

# 6. Stop ClickHouse
docker compose -f docker/clickhouse-compose.yml down
sleep 10
```

---

### 5C — Doris

Doris FE needs ~60 s to elect a leader after startup.

```bash
# 1. Stop any running containers
docker stop $(docker ps -q) 2>/dev/null
sleep 10

# 2. Start Doris FE + BE
cd /opt1/olap_poc/poc
docker compose -f docker/doris-compose.yml up -d

# 3. Wait for FE leader election (~60 s) and verify
sleep 60
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW FRONTENDS\G" 2>/dev/null \
  | grep -E "Alive|IsMaster"
# Expected: Alive: true  IsMaster: true

# 4. Run all 10 GCS queries
python3 gcs_testing/runner/run_gcs_benchmark.py --engine doris

# 5. Confirm result file
ls -lh gcs_testing/results/doris_gcs_*.jsonl

# 6. Stop Doris
docker compose -f docker/doris-compose.yml down
sleep 10
```

---

## Step 6 — Optional: Tuning the Run

```bash
# Run a subset of queries (useful for quick iteration)
python3 gcs_testing/runner/run_gcs_benchmark.py --engine duckdb \
    --queries GQ01,GQ03,GQ08

# Increase per-query timeout (default 600 s) for slow networks
python3 gcs_testing/runner/run_gcs_benchmark.py --engine clickhouse \
    --timeout 1200

# Increase warm iterations for a tighter median (default 3)
python3 gcs_testing/runner/run_gcs_benchmark.py --engine duckdb \
    --warm-iters 5

# Skip GQ10 (heavy scan) if RAM is critically low
python3 gcs_testing/runner/run_gcs_benchmark.py --engine duckdb \
    --queries GQ01,GQ02,GQ03,GQ04,GQ05,GQ06,GQ07,GQ08,GQ09

# Override warm iterations via environment variable
GCS_WARM_ITERATIONS=5 GCS_QUERY_TIMEOUT_SECONDS=900 \
    python3 gcs_testing/runner/run_gcs_benchmark.py --engine clickhouse
```

---

## Step 7 — Inspect Results

```bash
cd /opt1/olap_poc/poc

# List all GCS result files
ls -lh gcs_testing/results/

# Pretty-print a result file
cat gcs_testing/results/duckdb_gcs_*.jsonl | python3 -m json.tool | less

# Quick summary across all engines
for f in gcs_testing/results/*.jsonl; do
    echo ""
    echo "=== $(basename $f) ==="
    python3 - "$f" <<'EOF'
import json, sys
for line in open(sys.argv[1]):
    r = json.loads(line)
    qid    = r.get("query_id", "?")
    status = r.get("status", "?")
    warm   = r.get("warm_median_s")
    cold   = r.get("cold_s")
    rows   = r.get("rows_returned", "--")
    warm_s = f"{warm:.2f}s" if warm is not None else "--"
    cold_s = f"{cold:.2f}s" if cold is not None else "--"
    print(f"  {qid:<35} {status:<8} cold={cold_s:<10} warm={warm_s:<10} rows={rows}")
EOF
done
```

---

## Step 8 — Feed into Main Report (Optional)

The JSONL files from `gcs_testing/results/` use the same record schema as
the parent harness. Copy them into `poc/results/` to include them in the
`analyse_results.py` comparison report:

```bash
cp gcs_testing/results/*.jsonl results/
make analyse
# Outputs: report/01_raw_results.csv, 02_summary_table.md, 03_charts/, 04_final_report.md
```

---

## Query Reference

| ID | Name | What it measures |
|---|---|---|
| GQ01 | Full scan + agg | Raw GCS I/O throughput; COUNT / DISTINCT / MIN / MAX over full CSV |
| GQ02 | Filtered agg | `WHERE glusr_premium_enable='1'`; predicate over full scan (CSV has no skip index) |
| GQ03 | GROUP BY low-card | `GROUP BY category_type` (~5-10 distinct values); hash agg compute vs I/O |
| GQ04 | GROUP BY high-card | `GROUP BY fk_glusr_usr_id` (many users); memory pressure / spill |
| GQ05 | Date range filter | `BETWEEN` 2024-Q1 on `glusr_premium_added_date`; post-read filter, no partition pruning |
| GQ06 | TOP-N | Top 100 users by listing count; partial-sort / top-heap optimisation |
| GQ07 | String LIKE | `pl_kwrd_term_upper`, `glusr_premium_hist_comments`, `glusr_premium_updatedby_url`; I/O cost of widest columns |
| GQ08 | Approx distinct | HLL approximate vs exact DISTINCT; ClickHouse `uniq()` vs `uniqExact()` |
| GQ09 | Window function | Two-level CTE + `ROW_NUMBER` / running `SUM OVER PARTITION`; buffer-intensive |
| GQ10 | Heavy scan | Wide text + keyword/URL scans, 3-column CTE GROUP BY; designed to trigger spill |

---

## Result Record Schema

Each JSONL line has this structure:

```json
{
  "query_id":       "GQ01_full_scan_agg",
  "engine":         "duckdb",
  "status":         "OK",
  "cold_s":         42.3821,
  "warm_median_s":  38.4412,
  "warm_min_s":     37.2201,
  "warm_max_s":     40.0033,
  "rows_returned":  1,
  "oom":            false,
  "error":          null,
  "warm_iters":     3,
  "spill_bytes":    0,
  "gcs_prefix":     "pc_feature/GLUSR_PREMIUM_LISTING.csv",
  "timestamp":      "2026-04-20T10:23:00+00:00"
}
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `ERROR: Missing or placeholder GCS credentials` | `.env` not filled in | Set `GCS_GLUSR_PREMIUM_LISTING_PREFIX`, `GCS_HMAC_ACCESS_KEY`, `GCS_HMAC_SECRET` in `poc/.env` |
| `Connection refused` on Doris / ClickHouse | Engine container not running | `docker ps`; start the container (Step 5B or 5C) |
| `rows_returned = 0` on GQ01 | Wrong CSV path | Verify `GCS_GLUSR_PREMIUM_LISTING_PREFIX` with `gsutil ls gs://pc_feature/GLUSR_PREMIUM_LISTING.csv` |
| `OOM` on GQ04 or GQ10 | 8 GB RAM limit reached | Expected on constrained VM. ClickHouse has `SETTINGS max_bytes_before_external_group_by`; DuckDB spills to `/opt1/olap_poc/duckdb/spill`. Record the OOM and move on. |
| Query takes > 10 min on GQ01 | Slow GCS network | Increase `--timeout 1800`; check if other processes are saturating the NIC |
| `[!FAST]` flag on a result | Possible cache hit or empty result | Check `rows_returned`; if 0, the CSV prefix is wrong |
| DuckDB `HTTP Error 403` from GCS | HMAC key lacks read permission | Re-generate the HMAC key and grant `Storage Object Viewer` on the bucket |
| Doris `Access denied` on `s3()` TVF | Wrong key format or Doris version < 2.1 | Confirm `GCS_HMAC_ACCESS_KEY` starts with `GOOG`; upgrade Doris to 2.1+ |
| ClickHouse `FORMAT JSON` error | SQL file has trailing `;` or `--` comment | Check the last line of the failing `queries/GQxx/clickhouse.sql` |
| `SKIP` for every query | Wrong working directory | Run from `poc/` not from `poc/gcs_testing/` |
| DuckDB spill disk full | `/opt1` has < 2 GB free | `df -h /opt1`; delete old JSONL or DuckDB spill files in `/opt1/olap_poc/duckdb/spill/` |

---

## Key Paths

| Path | Purpose |
|---|---|
| `poc/.env` | Credentials and config — fill this in, never commit |
| `poc/gcs_testing/runner/run_gcs_benchmark.py` | The benchmark runner |
| `poc/gcs_testing/queries/GQxx_*/` | SQL files (3 dialects per query) |
| `poc/gcs_testing/results/*.jsonl` | Output — one file per run |
| `poc/gcs_testing/schema/glusr_premium_listing_gcs_spec.md` | Column type mapping and GCS TVF reference |
| `/opt1/olap_poc/duckdb/spill/` | DuckDB spill-to-disk directory (auto-created) |
