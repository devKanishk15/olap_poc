# OLAP Engine POC — Final Report

**Status**: TEMPLATE — populate with actual benchmark numbers after running the harness.

---

## 1. Executive Summary

> _(≤ 250 words — bottom line up front)_

This POC benchmarked three OLAP engines — **Apache Doris 2.1.7**, **DuckDB 1.2.1**, and **ClickHouse 24.12** — on a constrained single-node VM (4 vCPU / 8 GB RAM / 100 GB SSD) against a synthetic 10M-row, 60-column wide fact table (`event_fact`).

**Preliminary recommendation**: _(fill after results)_

| Rank | Engine | Strengths | Weaknesses |
|------|--------|-----------|------------|
| TBD | ClickHouse | Fastest aggregations, best GCS integration, mature ecosystem | Async mutations are an anti-pattern for updates; complex ops model |
| TBD | DuckDB | Simplest ops, excellent for analyst/embedded use, full ACID updates | No server mode; single-writer; GCS via httpfs is less mature |
| TBD | Doris | Best update semantics (MoW), MySQL-compatible, familiar SQL | Heaviest ops overhead; FE+BE architecture complexity on 8 GB |

> **Critical caveat**: All results are constrained by a **4 vCPU / 8 GB RAM** environment. Production hardware (32+ vCPU, 128+ GB RAM) would significantly change relative rankings, particularly for Doris which is architecturally designed for distributed scale.

---

## 2. Environment and Methodology

### Hardware
| Resource | Spec |
|----------|------|
| vCPU | 4 cores |
| RAM | 8 GB |
| Storage | 100 GB SSD at `/opt1` |
| OS | Ubuntu 22.04 LTS |
| Network | GCS via HTTPS (HMAC auth) |

### Engine Versions
| Engine | Version | Image/Binary |
|--------|---------|-------------|
| Apache Doris | 2.1.7 | `apache/doris:2.1.7-{fe,be}-x86_64` |
| DuckDB | 1.2.1 | CLI + Python `duckdb==1.2.1` |
| ClickHouse | 24.12 | `clickhouse/clickhouse-server:24.12` |

### Cold/Warm Protocol
- **1 cold run** per query: OS page cache dropped (`echo 3 > /proc/sys/vm/drop_caches`) + engine-level cache flush
- **5 warm runs** back-to-back without cache intervention
- **Reported metric**: median of the 5 warm runs (`warm_median_s`)
- **p95** of warm runs reported alongside median
- **Cold delta**: `cold_s - warm_median_s` — reflects cache warming benefit

### Fairness Rules Applied
1. Only one engine running at a time; others stopped and RAM verified free
2. All engines received identical data (same Parquet files, seed=42)
3. SQL semantics identical across dialects; dialect differences documented inline
4. Results marked `OOM` if query exceeded memory budget — never silently retried
5. Results flagged `[!FAST]` if < 5 ms (possible cache hit or no-op)
6. Memory limits enforced at container + engine level (see install scripts)

---

## 3. Read Benchmark Results — Local Mode

> Fill from `02_summary_table.md` after running `analyse_results.py`

### 3.1 Full Scan Aggregation (Q01)

| Engine | Cold (s) | Warm Median (s) | Warm p95 (s) | Cold Δ (s) | Spill |
|--------|----------|-----------------|-------------|-----------|-------|
| Doris | _TBD_ | _TBD_ | _TBD_ | _TBD_ | — |
| DuckDB | _TBD_ | _TBD_ | _TBD_ | _TBD_ | — |
| ClickHouse | _TBD_ | _TBD_ | _TBD_ | _TBD_ | — |

### 3.2 Filtered Aggregate (Q02)

| Engine | Warm Median (s) | Warm p95 (s) | Spill |
|--------|-----------------|-------------|-------|
| Doris | _TBD_ | _TBD_ | — |
| DuckDB | _TBD_ | _TBD_ | — |
| ClickHouse | _TBD_ | _TBD_ | — |

### 3.3 GROUP BY Low Cardinality (Q03)

_(table TBD)_

### 3.4 GROUP BY High Cardinality (Q04) — Spill Candidate

_(table TBD — note any OOM events and spill size)_

### 3.5 Date-Range Scan (Q05)

_(table TBD — note partition pruning efficiency: expected ~23% of data scanned)_

### 3.6 TOP-N (Q06)

_(table TBD — note whether engine applied heap optimisation vs full sort)_

### 3.7 Small Dimension Join (Q07)

_(table TBD)_

### 3.8 String LIKE / Regex (Q08)

_(table TBD — note ClickHouse match() vs LIKE performance delta)_

### 3.9 Approximate Distinct Count (Q09)

_(table TBD — compare approx vs exact; note accuracy vs speed trade-off)_

### 3.10 Window Function (Q10)

_(table TBD)_

### 3.11 JSON Field Extraction (Q11)

_(table TBD — note ClickHouse JSONExtractString vs DuckDB ->> vs Doris JSON_EXTRACT_STRING)_

### 3.12 Heavy Multi-Level Aggregation (Q12) — Spill Candidate

_(table TBD — expected to spill on all engines; document spill sizes and latency penalty)_

### 3.13 Multi-Dimension GROUP BY (Q13)

_(table TBD)_

---

## 4. Read Benchmark Results — GCS Mode

> Q14 and all queries run against GCS-resident Parquet files.

### 4.1 Full Scan via GCS (Q14)

| Engine | Mechanism | Warm Median (s) | vs Local Δ |
|--------|-----------|-----------------|-----------|
| Doris | `s3()` TVF | _TBD_ | _TBD_ |
| DuckDB | `read_parquet('s3://...')` + httpfs | _TBD_ | _TBD_ |
| ClickHouse | `s3()` table function | _TBD_ | _TBD_ |

**GCS Integration Maturity Notes:**
- **ClickHouse**: Native `s3()` function, well-tested, supports glob patterns
- **DuckDB**: httpfs extension; HMAC or SA JSON auth; metadata caching available
- **Doris**: TVF approach works; External Catalog is more idiomatic for repeated access

---

## 5. Write Benchmark Results

### 5.1 W1 — Bulk Load (10M rows)

| Engine | Rows Loaded | Total Time (s) | rows/s | Notes |
|--------|-------------|---------------|--------|-------|
| Doris | _TBD_ | _TBD_ | _TBD_ | Stream load API |
| DuckDB | _TBD_ | _TBD_ | _TBD_ | INSERT FROM read_parquet() |
| ClickHouse | _TBD_ | _TBD_ | _TBD_ | HTTP INSERT FORMAT Parquet |

### 5.2 W2 — Micro-Batch Insert (10k rows × 50 batches)

| Engine | Median Batch (ms) | p95 Batch (ms) | Total rows/s |
|--------|------------------|---------------|-------------|
| Doris | _TBD_ | _TBD_ | _TBD_ |
| DuckDB | _TBD_ | _TBD_ | _TBD_ |
| ClickHouse | _TBD_ | _TBD_ | _TBD_ |

### 5.3 W3 — Point Update (1,000 single-row updates)

| Engine | Status | Median (ms) | p95 (ms) | Semantic Note |
|--------|--------|-------------|----------|---------------|
| Doris | _TBD_ | — | — | FEATURE_GAP on Duplicate Key; OK on Unique Key MoW |
| DuckDB | OK | _TBD_ | _TBD_ | Standard MVCC UPDATE |
| ClickHouse | OK | _TBD_ (submit) | _TBD_ (complete) | Async mutation — submit fast, completion slow |

### 5.4 W4 — Bulk Update (~5% of rows)

| Engine | Rows Affected | Total Time (s) | rows/s | Semantic Note |
|--------|--------------|---------------|--------|---------------|
| Doris | _TBD_ | _TBD_ | _TBD_ | FEATURE_GAP on Duplicate Key |
| DuckDB | _TBD_ | _TBD_ | _TBD_ | Full MVCC, predicate pushdown |
| ClickHouse | _TBD_ | _TBD_ (complete) | _TBD_ | Async mutation rewrites parts |

---

## 6. Memory and Stability Observations

### OOM Events
| Query | Engine | Status | Notes |
|-------|--------|--------|-------|
| _TBD_ | _TBD_ | OOM / OK | _TBD_ |

### Spill Events
| Query | Engine | Spill Size | Latency Overhead |
|-------|--------|-----------|-----------------|
| Q04 | _TBD_ | _TBD_ MB | _TBD_× vs median |
| Q12 | _TBD_ | _TBD_ MB | _TBD_× vs median |

### Stability Notes
- _Any container restarts, BE crashes, or unexpected exits_
- _DuckDB WAL or corruption events_
- _ClickHouse mutation queue backlog_

---

## 7. Qualitative Comparison

### Feature Parity Matrix

| Feature | Doris | DuckDB | ClickHouse |
|---------|-------|--------|------------|
| Standard SQL UPDATE | ✅ (Unique Key MoW only) | ✅ | ⚠️ Async mutation |
| Standard SQL DELETE | ✅ (MoW) | ✅ | ⚠️ Async mutation |
| ACID transactions | ⚠️ Partial | ✅ | ❌ |
| Window functions | ✅ | ✅ | ✅ (v21.3+) |
| JSON type / functions | ✅ | ✅ | ✅ (via String) |
| GCS native read | ✅ TVF | ✅ httpfs | ✅ s3() |
| GCS native write | ⚠️ External table | ✅ COPY TO | ✅ s3() |
| Approximate COUNT DISTINCT | ✅ HLL | ✅ APPROX_COUNT_DISTINCT | ✅ uniq() |
| Materialized views | ✅ | ⚠️ (manual) | ✅ |
| Partitioning | ✅ RANGE/LIST | ❌ (file-level) | ✅ |
| Cluster/distributed | ✅ (designed for) | ❌ | ✅ |
| MySQL-compatible protocol | ✅ | ❌ | ⚠️ (limited) |
| Server mode | ✅ | ❌ (in-process) | ✅ |

### Operability
| Dimension | Doris | DuckDB | ClickHouse |
|-----------|-------|--------|-----------|
| Setup complexity | High (FE+BE) | Very Low | Low–Medium |
| Monitoring / observability | ✅ (system tables) | ⚠️ (limited) | ✅ (system tables) |
| Schema migration | ✅ ALTER TABLE | ✅ | ✅ |
| Upgrade path | Moderate | Simple | Simple |

---

## 8. Recommendation

> _(Fill after results — scored decision matrix)_

### Decision Matrix

| Criterion | Weight | Doris | DuckDB | ClickHouse |
|-----------|--------|-------|--------|-----------|
| Read latency (aggregations) | 30% | /10 | /10 | /10 |
| GCS read performance | 20% | /10 | /10 | /10 |
| Write/update capability | 20% | /10 | /10 | /10 |
| Operability on 8 GB | 15% | /10 | /10 | /10 |
| Ecosystem maturity | 10% | /10 | /10 | /10 |
| SQL compatibility | 5% | /10 | /10 | /10 |
| **Weighted Total** | 100% | **/10** | **/10** | **/10** |

**Recommendation**: _(engine)_ is recommended because _(rationale)_.

---

## 9. Caveats — 4 vCPU / 8 GB Constraints

The following conclusions from this POC are likely to **change significantly on production hardware**:

1. **Doris FE+BE overhead**: On 8 GB, Doris loses ~1.5 GB to coordination overhead that scales sub-linearly on larger nodes. On 32+ GB, this overhead becomes negligible.
2. **High-cardinality GROUP BY (Q04, Q12)**: Spill behaviour at 8 GB will not occur at 128 GB. ClickHouse and Doris are likely to close the gap with DuckDB on larger RAM.
3. **Bulk load throughput (W1)**: Doris is designed for parallel FE‑coordinated stream load across multiple BEs. Single-node numbers are not representative.
4. **ClickHouse mutation latency (W3/W4)**: Part-file rewrite time scales with data volume and number of parts, not RAM. This remains a semantic concern regardless of hardware.
5. **DuckDB concurrency**: DuckDB is single-writer; in a multi-user environment, this becomes a hard architectural limit that hardware cannot solve.

---

## 10. Appendix

### A. Exact Engine Versions and Config
_(Copy from .env)_

### B. Raw Numbers
See `01_raw_results.csv` for all JSONL results merged into tabular format.

### C. Query Dialect Differences Summary

| Query | Doris | DuckDB | ClickHouse |
|-------|-------|--------|-----------|
| Distinct count | `COUNT(DISTINCT x)` | `COUNT(DISTINCT x)` | `uniqExact(x)` |
| Approx distinct | `APPROX_COUNT_DISTINCT(x)` | `APPROX_COUNT_DISTINCT(x)` | `uniq(x)` |
| Percentile | `PERCENTILE_APPROX(x, 0.95)` | `APPROX_QUANTILE(x, 0.95)` | `quantile(0.95)(x)` |
| Conditional count | `SUM(CASE WHEN ... END)` | `SUM(CASE WHEN ... END)` | `countIf(...)` |
| JSON extract | `JSON_EXTRACT_STRING(col, '$.key')` | `col ->> '$.key'` | `JSONExtractString(col, 'key')` |
| NULL check | `IS NOT NULL` | `IS NOT NULL` | `isNotNull(col)` |
| GCS table func | `s3("uri", "key", "sec", "parquet")` TVF | `read_parquet('s3://...')` | `s3('https://...', 'key', 'sec', 'Parquet')` |
