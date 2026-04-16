# OLAP POC — Benchmark Summary

_Generated: 2026-04-15T20:36:00.012099Z_

---

## Read Benchmarks — Local Mode

### Mode: `local`

| Query                 | Doris (med/p95)   | DuckDB (med/p95)   | ClickHouse (med/p95)   |
|-----------------------|-------------------|--------------------|------------------------|
| Q01_full_agg          | 1.711s / 1.760s   | 0.001s / 0.001s    | 0.240s / 0.242s        |
| Q02_filtered_agg      | 0.174s / 0.185s   | 0.002s / 0.002s    | 0.017s / 0.017s        |
| Q03_groupby_low_card  | 2.482s / 2.546s   | 0.001s / 0.001s    | 0.624s / 0.683s        |
| Q04_groupby_high_card | nans / nans  🌊    | 0.002s / 0.002s    | 2.888s / 2.928s        |
| Q05_date_range        | 0.808s / 0.815s   | 0.001s / 0.002s    | 0.090s / 0.090s        |
| Q06_topn              | 1.970s / 2.014s   | 0.002s / 0.002s    | 0.545s / 0.559s        |
| Q07_join              | 4.778s / 4.792s   | 0.003s / 0.003s    | 1.108s / 1.126s        |
| Q08_string_like       | 1.665s / 1.684s   | 0.002s / 0.002s    | 0.920s / 0.961s        |
| Q09_approx_distinct   | 8.063s / 8.068s   | 0.001s / 0.001s    | 1.317s / 1.318s        |
| Q10_window_func       | 3.264s / 3.271s   | 0.002s / 0.002s    | 0.351s / 0.365s        |
| Q11_json_extract      | 3.092s / 3.138s   | 0.001s / 0.002s    | 2.000s / 2.055s        |
| Q12_heavy_spill       | nans / nans  🌊    | 0.002s / 0.002s    | 12.241s / 12.577s      |
| Q13_multi_dim_groupby | 2.467s / 2.551s   | 0.002s / 0.002s    | 0.611s / 0.652s        |

🌊 = spill to disk observed

### Mode: `gcs`

_No results yet._

---

## Write Benchmarks


### `W1_bulk_load`

| Engine     | Status   | rows/s   |   Notes |
|------------|----------|----------|---------|
| clickhouse | OK       | 136,008  |     nan |
| doris      | OK       | 33,928   |     nan |
| duckdb     | OK       | 211,189  |     nan |



### `W2_micro_batch`

| Engine     | Status   | rows/s   |   Notes |
|------------|----------|----------|---------|
| clickhouse | OK       | 4,929    |     nan |
| doris      | OK       | 3,864    |     nan |
| duckdb     | OK       | 4,303    |     nan |



### `W3_point_update`

| Engine     | Status      |   rows/s | Notes                                                                            |
|------------|-------------|----------|----------------------------------------------------------------------------------|
| clickhouse | OK          |      nan | ClickHouse mutations are ASYNCHRONOUS. Submission is fast (~ms); completion rewr |
| doris      | FEATURE_GAP |      nan | Doris DUPLICATE KEY tables do not support row-level UPDATE. Use event_fact_mow ( |
| duckdb     | OK          |      nan | Standard MVCC UPDATE — fully ACID, in-place.                                     |



### `W4_bulk_update`

| Engine     | Status      | rows/s   | Notes                                                                            |
|------------|-------------|----------|----------------------------------------------------------------------------------|
| clickhouse | OK          | 60,611   | ClickHouse ALTER TABLE ... UPDATE is an ASYNC mutation that rewrites affected pa |
| doris      | FEATURE_GAP | nan      | Doris DUPLICATE KEY model does not support UPDATE statements. Bulk updates requi |
| duckdb     | OK          | 83,137   | Standard MVCC bulk UPDATE — ACID, full predicate pushdown.                       |

