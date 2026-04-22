# OLAP Engine POC — Final Benchmark Report

This folder consolidates **local** and **GCS** benchmark results for Apache Doris 2.1.7, DuckDB 1.2.1, and ClickHouse 24.12 on a 4 vCPU / 8 GB RAM / 100 GB SSD VM.

## Contents

| File | Purpose |
|------|---------|
| [`index.html`](index.html) | **Interactive dashboard** — open in a browser. Log/linear toggles, per-query deep dive, engine architecture notes. |
| [`00_benchmark_methodology.md`](00_benchmark_methodology.md) | What every query and workload actually does — Q01–Q13, GQ01–GQ10, W1–W4. |
| [`01_engines_explained.md`](01_engines_explained.md) | Conceptual primer on each engine's architecture and trade-offs. |
| [`02_local_analysis.md`](02_local_analysis.md) | Analysis of local Parquet read benchmarks + write workloads. |
| [`03_gcs_analysis.md`](03_gcs_analysis.md) | Analysis of direct-from-GCS CSV read benchmarks. |
| [`04_summary.md`](04_summary.md) | Consolidated one-page summary and verdict. |
| [`charts/`](charts/) | PNG charts (local latency, GCS latency, GCS cold-vs-warm, GCS totals, write throughput). |

## Source data

- Local:  `poc/report/01_raw_results.csv`
- GCS:    `poc/gcs_testing/results/{doris,duckdb,clickhouse}_gcs_*.jsonl`

## Headline numbers

| Category | Winner | Runner-up | Laggard |
|----------|--------|-----------|---------|
| Local reads (warm median, 13 queries) | DuckDB¹ | ClickHouse | Doris |
| Local reads — queries that **completed** | ClickHouse (13/13) | DuckDB (13/13) | Doris (11/13, 2 OOM) |
| GCS reads (warm median, 10 queries) | **Doris** | ClickHouse | DuckDB |
| Bulk load throughput (W1) | DuckDB 211k rows/s | ClickHouse 136k rows/s | Doris 34k rows/s |
| Update semantics | DuckDB (ACID MVCC) | ClickHouse (async mutation) | Doris (feature gap on DUPLICATE KEY) |

¹ DuckDB's sub-millisecond warm runs benefit from its in-process query-cache hot path — treat as a best case.
