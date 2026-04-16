# GCS Read Benchmarks — `pc_item_image`

Self-contained benchmark suite that queries the **`pc_item_image`** production table
**directly from GCS** (no local data loading) across Apache Doris, DuckDB, and ClickHouse.

This folder is independent of the parent `poc/` write benchmarks. It requires only a
running engine and valid GCS HMAC credentials.

---

## Prerequisites

| Requirement | Details |
|---|---|
| Python | 3.10+ |
| Engine | One of Doris, DuckDB, ClickHouse — see `poc/scripts/0{1,2,3}_install_*.sh` |
| GCS access | HMAC key with read permission on the `pc_item_image` CSV bucket |
| `.env` file | `poc/.env` populated — see section below |

---

## Environment Setup

```bash
# Install minimal runner dependencies (or reuse the parent venv)
pip install -r runner/requirements.txt

# Or with the parent harness venv already active:
# /opt1/poc/.venv/bin/pip install -r runner/requirements.txt
```

---

## Configuration (`.env`)

The runner reads from `poc/.env` (same file the parent harness uses).  
One new variable is required for this benchmark suite:

```bash
# In poc/.env — add this line:
GCS_PC_ITEM_IMAGE_PREFIX=pc_feature/PC_ITEM_IMAGE.csv
```

| Variable | Required | Description |
|---|---|---|
| `GCS_BUCKET` | Yes | GCS bucket name, e.g. `pc_feature` |
| `GCS_HMAC_ACCESS_KEY` | Yes | HMAC key ID (begins with `GOOG...`) |
| `GCS_HMAC_SECRET` | Yes | HMAC key secret |
| `GCS_PC_ITEM_IMAGE_PREFIX` | **Yes — new** | Key path within the bucket, e.g. `pc_feature/PC_ITEM_IMAGE.csv`. May include a glob pattern if the export is split across multiple files, e.g. `pc_feature/PC_ITEM_IMAGE_*.csv`. Do **not** include the bucket name. |
| `GCS_REGION` | No | Defaults to `auto` (recommended for GCS) |
| `DORIS_HOST`, `DORIS_FE_QUERY_PORT`, `DORIS_USER`, `DORIS_PASSWORD` | Doris only | Standard Doris connection params |
| `CLICKHOUSE_HOST`, `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | ClickHouse only | Standard ClickHouse HTTP connection params |

---

## Running Benchmarks

```bash
cd poc/gcs_testing

# Full run — all 10 queries, one engine
python runner/run_gcs_benchmark.py --engine duckdb
python runner/run_gcs_benchmark.py --engine clickhouse
python runner/run_gcs_benchmark.py --engine doris

# Run a subset of queries
python runner/run_gcs_benchmark.py --engine duckdb --queries GQ01,GQ03,GQ08

# Dry run — prints substituted SQL for every query, no engine needed
python runner/run_gcs_benchmark.py --engine clickhouse --dry-run
python runner/run_gcs_benchmark.py --engine duckdb --dry-run

# Custom timeout (seconds per query; default 600)
python runner/run_gcs_benchmark.py --engine doris --timeout 900

# Custom warm iterations (default 3)
python runner/run_gcs_benchmark.py --engine duckdb --warm-iters 5
```

---

## Query Reference

| ID | Name | What it measures |
|---|---|---|
| GQ01 | Full scan + agg | Raw GCS-to-engine I/O throughput; COUNT/DISTINCT/MIN/MAX over full ~70 GB CSV |
| GQ02 | Filtered agg | Predicate pushdown effectiveness; all rows still scanned (CSV has no skip index) |
| GQ03 | GROUP BY low-card | Hash aggregation over `pc_item_img_status` (~5 values); compute vs I/O ratio |
| GQ04 | GROUP BY high-card | Hash aggregation over `pc_item_image_glusr_id` (many sellers); memory pressure |
| GQ05 | Date range filter | Time-bounded scan; measures post-read filter performance, no partition pruning |
| GQ06 | TOP-N | Top 100 sellers by image count; tests partial-sort / top-heap optimisation |
| GQ07 | String LIKE | URL column scans + `hist_comments` (widest columns); I/O-heavy |
| GQ08 | Approx distinct | HLL approximate vs exact distinct count; ClickHouse uses `uniq()` vs `uniqExact()` |
| GQ09 | Window function | Two-level CTE + ROW_NUMBER / running SUM OVER PARTITION; buffer-intensive |
| GQ10 | Heavy scan | All 6 URL columns + comments, 3-col GROUP BY in CTE; designed to trigger spill |

---

## Results

Results are written as JSONL to `gcs_testing/results/<engine>_gcs_<timestamp>.jsonl`.

**Record schema:**
```json
{
  "query_id":       "GQ01_full_scan_agg",
  "engine":         "duckdb",
  "status":         "OK",
  "cold_s":         142.38,
  "warm_median_s":  118.44,
  "warm_min_s":     116.22,
  "warm_max_s":     121.00,
  "rows_returned":  1,
  "oom":            false,
  "error":          null,
  "warm_iters":     3,
  "spill_bytes":    0,
  "gcs_prefix":     "pc_feature/PC_ITEM_IMAGE.csv",
  "timestamp":      "2026-04-17T10:23:00+00:00"
}
```

**Status values:**
- `OK` — completed all warm iterations
- `OOM` — ran out of memory; stops iterating, records `"oom": true`
- `ERROR` — non-OOM engine error
- `SKIP` — SQL file not found for this engine

---

## Iteration Protocol

| Iteration | Cold/Warm | Description |
|---|---|---|
| 1 | **Cold** | First run after connection open. No OS cache drop (data is remote). DuckDB: fresh in-process connection. ClickHouse: `SYSTEM DROP DNS CACHE` + `SYSTEM DROP MARK CACHE`. |
| 2–4 | **Warm** | Repeated executions; engine may cache query plans or HTTP connections. |

The **warm median** is the headline number. Cold delta = cold_s − warm_median_s.

---

## Dialect Differences Summary

| Feature | Doris | DuckDB | ClickHouse |
|---|---|---|---|
| GCS access | `s3()` TVF, `s3://` URI + `s3.endpoint` | `read_csv_auto('s3://')` + httpfs | `s3('https://')` |
| `COUNT(*)` | `COUNT(*)` | `COUNT(*)` | `count()` |
| `COUNT(DISTINCT x)` | `COUNT(DISTINCT x)` | `COUNT(DISTINCT x)` | `uniqExact(x)` |
| Approx distinct | `APPROX_COUNT_DISTINCT(x)` | `APPROX_COUNT_DISTINCT(x)` | `uniq(x)` (HLL) |
| `SUM(CASE WHEN ...)` | `SUM(CASE WHEN ...)` | `SUM(CASE WHEN ...)` | `countIf(...)` |
| `IS NOT NULL` | `IS NOT NULL` | `IS NOT NULL` | `isNotNull()` |
| `CAST(col AS DATE)` | `CAST(col AS DATE)` | `CAST(col AS DATE)` | `toDate(col)` |
| Memory spill config | engine-level | `SET memory_limit` | `SETTINGS max_bytes_before_external_group_by` |

Full reference: `poc/schema/DIALECT_DIFFERENCES.md`

---

## Known Limitations

1. **No skip-index / partition pruning in CSV**: GQ05 (date range) scans the entire file regardless of the `WHERE` clause. Results reflect network I/O + filter, not selective scan.
2. **Cold run ≠ true cold on GCS**: GCS CDN/edge caching may warm up between runs. Cold run latency reflects first-connection overhead rather than a truly cold object store read.
3. **DuckDB `read_csv_auto` sampling**: The explicit `columns` dict in all DuckDB SQL files prevents type misdetection, but DuckDB's CSV reader still buffers the full connection stream — it cannot skip columns. All 35 columns are always transferred.
4. **Doris `"columns"` parameter requires Doris 2.1+**: Earlier versions may not support this TVF parameter; test with `SELECT *` and positional aliases if you downgrade.
5. **OOM on GQ04 / GQ10 at 8 GB RAM**: Expected on a constrained VM. ClickHouse uses `SETTINGS max_bytes_before_external_group_by` to spill rather than OOM. DuckDB spills to `DUCKDB_SPILL_DIR`. Doris behaviour depends on BE memory settings.
6. **Timestamp range in GQ05**: Hardcoded to 2024-Q1. Update the `BETWEEN` clause if the actual data spans a different date range.
