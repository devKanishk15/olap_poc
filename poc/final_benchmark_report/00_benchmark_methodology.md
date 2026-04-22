# Benchmark Methodology — What We Actually Ran

This document catalogues every read query and write workload executed in this POC. For each one: what it does, what it stresses, and why it is in the benchmark.

- **Dataset (local):** `event_fact` — 10M rows, 60 columns, Hive-partitioned by `event_date`. Generated deterministically with seed=42.
- **Dataset (GCS):** `GLUSR_PREMIUM_LISTING.csv` on bucket `apachedorispoc`, read directly over HTTPS with HMAC credentials.
- **Protocol:** Each read query runs 1 cold run (OS page cache + engine cache flushed) + 5 warm runs (local) or 3 warm runs (GCS). Reported metric is the **warm median**.
- **Execution:** one engine at a time; others stopped and RAM verified free.

---

## 1. Local Read Queries (Q01–Q13)

All queries run against the local Parquet `event_fact` table. Each is written in three dialects (Doris, DuckDB, ClickHouse) kept semantically identical — dialect variance documented in `schema/DIALECT_DIFFERENCES.md`.

### Q01 — Full Scan Aggregation
```sql
SELECT COUNT(*), SUM(revenue), AVG(duration_ms),
       MIN(event_ts), MAX(event_ts), COUNT(DISTINCT user_id)
FROM event_fact;
```
- **What it tests:** raw columnar scan speed + a handful of simple aggregates.
- **Stresses:** decompression throughput and `COUNT(DISTINCT)` implementation (exact vs HLL).
- **Why:** the baseline "how fast can you read everything?" query.

### Q02 — Filtered Aggregate
Narrow 7-day window + `event_type = 'purchase'` → aggregates by `(event_type, country_code)`.
- **Tests:** predicate pushdown, min/max zone-map skipping, partition-file pruning.
- **Stresses:** how well the engine avoids reading data it doesn't need.

### Q03 — GROUP BY Low Cardinality
`GROUP BY event_type` (a handful of distinct values).
- **Tests:** hot-path hash aggregation where the hash table fits in L3 cache.
- **Stresses:** per-group compute efficiency.

### Q04 — GROUP BY High Cardinality
`GROUP BY user_id` (millions of distinct values).
- **Tests:** large hash tables that spill memory.
- **Stresses:** memory budget + spill-to-disk behaviour.
- **Result:** Doris OOM'd; ClickHouse and DuckDB completed.

### Q05 — Date-Range Scan
Covers a 7-day slice (~23% of data).
- **Tests:** partition pruning on `event_date` (Hive-partitioned Parquet).
- **Stresses:** metadata-driven file elimination.

### Q06 — TOP-N
`ORDER BY session_revenue DESC LIMIT 100` with a pre-aggregation by session.
- **Tests:** whether the engine uses a TOP-N heap or does a full sort.
- **Stresses:** sort-merge vs heap optimisation path.

### Q07 — Small-Dimension Join
Joins fact to a tiny 9-row channel-label CTE.
- **Tests:** broadcast-hash-join optimisation, join planner choice.
- **Stresses:** ability to recognise the small side and avoid a shuffle.

### Q08 — String LIKE / Pattern Match
Multiple `LIKE '%…%'` predicates on `user_agent` and `referrer_url`.
- **Tests:** string-column scan speed + substring matching.
- **Stresses:** text decoding, vectorised string ops.

### Q09 — Approximate Distinct (HLL)
`APPROX_COUNT_DISTINCT(user_id)` grouped by date & event_type.
- **Tests:** HyperLogLog sketch implementation.
- **Stresses:** the engine's own approximate-distinct operator (not generic COUNT DISTINCT).

### Q10 — Window Function
`SUM() OVER (PARTITION BY event_type ORDER BY event_date)` + rank.
- **Tests:** ANSI window function correctness and sort-group efficiency.
- **Stresses:** partition-sort + running-aggregate pipeline.

### Q11 — JSON Extract
`json_extract(custom_dimensions, '$.plan')` over a JSON-typed column.
- **Tests:** JSON-column storage + path extraction performance.
- **Stresses:** per-row JSON parse; engines with native JSON types win here.

### Q12 — Heavy Spill (Intentional OOM Candidate)
Two-level CTE: user-daily aggregate → quantile + top segments.
- **Tests:** what happens when working-set > 8 GB RAM.
- **Stresses:** spill-to-disk path robustness.
- **Result:** Doris OOM'd; ClickHouse spilled and completed in ~12 s; DuckDB completed warm.

### Q13 — Multi-Dim GROUP BY
`GROUP BY product_category_l1, campaign_channel, ab_variant, device_type`.
- **Tests:** multi-dimensional rollup / grouping-sets execution.
- **Stresses:** combined cardinality of the key space.

### Q14 — GCS Remote Read (runs in GCS mode only)
A targeted CSV read that was the precursor to the full GCS suite (GQ01–GQ10). Kept for regression tracking only.

---

## 2. GCS Read Queries (GQ01–GQ10)

All queries read directly from the `apachedorispoc` GCS bucket via:
- Doris → `s3(...)` TVF with HMAC,
- DuckDB → `read_csv_auto('s3://...')` via `httpfs` extension,
- ClickHouse → `s3(...)` TVF with HMAC.

Credentials are injected at runtime by the harness (`<GCS_BUCKET>`, `<GCS_HMAC_ACCESS_KEY>`, `<GCS_HMAC_SECRET>` token replacement).

| # | Query | Analogue to | What it tests |
|---|-------|-------------|---------------|
| GQ01 | full scan + agg | Q01 | raw remote I/O throughput over HTTPS |
| GQ02 | filtered agg | Q02 | predicate pushdown into the CSV reader |
| GQ03 | groupby low card | Q03 | aggregation on streamed CSV rows |
| GQ04 | groupby high card | Q04 | hash-table size vs constrained memory |
| GQ05 | date range | Q05 | filter-while-streaming (no partition pruning — CSV is one file) |
| GQ06 | topn | Q06 | sort/heap over streamed data |
| GQ07 | string like | Q08 | substring matching mid-stream |
| GQ08 | approx distinct | Q09 | HLL on a remote stream |
| GQ09 | window func | Q10 | whether window functions force full materialisation |
| GQ10 | heavy scan | Q12 | two-level CTE to push memory boundary with remote I/O |

The key difference vs local is that **network transfer dominates**. Engine differences here reflect how efficiently they parallelise the CSV download and parse, not how good their execution engine is.

---

## 3. Write Workloads (W1–W4)

Four Python drivers (`workloads/W*.py`), each invoked as a subprocess by the harness. Each emits a JSON result line on stdout.

### W1 — Bulk Load (`W1_bulk_load.py`)
Loads all 10M rows from local Parquet files into the engine's native table.

- **Doris:** Stream Load via HTTP to the FE (NDJSON chunks → BE).
- **DuckDB:** `COPY event_fact FROM 'parquet_glob' (FORMAT PARQUET)`.
- **ClickHouse:** Native Parquet insert via `INSERT ... FROM INFILE` / HTTP bulk.
- **Metric:** rows/second wall-time.
- **What it tells you:** cold-start ingestion throughput — the relevant number when rebuilding a table from cold storage.

### W2 — Micro-Batch Streaming (`W2_micro_batch.py`)
Generates N batches of 10k synthetic rows with a different seed, pushed as rapid small inserts to simulate streaming ingest.

- **Doris:** stream-load one NDJSON chunk per batch.
- **DuckDB:** `INSERT INTO ... VALUES (...)` batched.
- **ClickHouse:** HTTP `INSERT` per batch, native binary format.
- **Metrics:** rows/s, median batch-ms, p95 batch-ms.
- **What it tells you:** per-request overhead and tail latency under sustained streaming load.

### W3 — Point Update (`W3_point_update.py`)
UPDATEs a single row by primary key, repeated 1000 times across sampled event_ids.

- **DuckDB:** standard `UPDATE ... WHERE event_id = ?` — ACID MVCC, in-place.
- **ClickHouse:** `ALTER TABLE ... UPDATE ... WHERE ...` — this is an **async mutation**. The harness submits all mutations first (submission is ~ms), then polls `system.mutations` once at the end to measure completion time.
- **Doris:** the fact table uses `DUPLICATE KEY` which does not support row-level UPDATE → recorded as `FEATURE_GAP`. (A UNIQUE KEY Merge-on-Write table would support it but changes the read profile.)
- **Metrics:** submit median/p95 ms, completion total seconds.
- **What it tells you:** whether the engine offers real transactional update semantics.

### W4 — Bulk Update (`W4_bulk_update.py`)
A single `UPDATE ... WHERE` touching ~5% of rows (~500k) — simulates re-tagging a segment.

- **DuckDB:** one big ACID MVCC UPDATE with predicate pushdown.
- **ClickHouse:** one ALTER mutation that rewrites affected parts — async, eventually consistent.
- **Doris:** same FEATURE_GAP as W3 on DUPLICATE KEY.
- **Metrics:** rows affected / second, total elapsed.
- **What it tells you:** throughput for bulk corrections/reprocessing.

---

## 4. Why this query set?

Coverage is intentional — each query targets a distinct execution pattern an OLAP engine claims to do well:

| Execution pattern | Covered by |
|-------------------|-----------|
| Raw scan throughput | Q01, GQ01 |
| Predicate pushdown / zone maps | Q02, GQ02 |
| Partition pruning | Q05 |
| Hash aggregation (small HT) | Q03, GQ03 |
| Hash aggregation (large HT) | Q04, GQ04 |
| TOP-N optimisation | Q06, GQ06 |
| Broadcast hash join | Q07 |
| String / pattern match | Q08, GQ07 |
| Approximate-distinct sketches | Q09, GQ08 |
| Window / analytic functions | Q10, GQ09 |
| Semi-structured (JSON) access | Q11 |
| Spill-to-disk robustness | Q12, GQ10 |
| Multi-dimensional rollup | Q13 |
| Bulk ingestion | W1 |
| Streaming ingestion | W2 |
| Row-level UPDATE | W3 |
| Bulk UPDATE | W4 |

Everything else (caching, partition maintenance, materialised views) is deliberately **out of scope** for this POC to keep fairness tractable on a single VM. Where those features matter, see `04_summary.md` → "Where to go next."

---

## 5. What the harness records

Each run emits a JSONL line per query/workload. For read queries the schema is:

```
query_id, engine, mode, status, cold_s, warm_median_s, warm_p95_s,
warm_min_s, warm_max_s, cold_vs_warm, rows_returned, spill, warm_iters,
timestamp
```

For writes, workload-specific fields are also present (`rows_per_s`, `batches`, `submit_median_ms`, `completion_total_s`, `semantic_note`, etc.).

`status` is one of: `OK`, `OOM`, `FEATURE_GAP`, `ERROR`, `TIMEOUT` — with no silent retries. A query that fails shows up as a gap in the charts rather than a fabricated number.
