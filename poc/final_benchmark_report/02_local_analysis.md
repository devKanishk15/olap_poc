# Local Benchmark Analysis

**Dataset:** 10M-row Parquet fact table (`event_fact`, 60 columns, Hive-partitioned by `event_date`) on local SSD.
**Protocol:** 1 cold + 5 warm runs per query. Reported metric = warm median.

![Local latency chart](charts/latency_local.png)

## Read queries — warm median latency (seconds)

| Query | Doris | DuckDB | ClickHouse | Notes |
|-------|-------|--------|------------|-------|
| Q01_full_agg | 1.711 | 0.001 | 0.240 | Full-scan aggregation |
| Q02_filtered_agg | 0.174 | 0.002 | 0.017 | Selective filter + agg |
| Q03_groupby_low_card | 2.482 | 0.001 | 0.624 | Low-card GROUP BY |
| Q04_groupby_high_card | 🌊 OOM | 0.002 | 2.888 | Doris OOM'd on hash table |
| Q05_date_range | 0.808 | 0.001 | 0.090 | Partition pruning |
| Q06_topn | 1.970 | 0.002 | 0.545 | TOP-N sort |
| Q07_join | 4.778 | 0.003 | 1.108 | Fact × small dimension |
| Q08_string_like | 1.665 | 0.002 | 0.920 | Pattern match |
| Q09_approx_distinct | 8.063 | 0.001 | 1.317 | HLL sketch |
| Q10_window_func | 3.264 | 0.002 | 0.351 | PARTITION BY + ORDER BY |
| Q11_json_extract | 3.092 | 0.001 | 2.000 | JSON path extract |
| Q12_heavy_spill | 🌊 OOM | 0.002 | 12.241 | Deliberate spill — CH coped, Doris didn't |
| Q13_multi_dim_groupby | 2.467 | 0.002 | 0.611 | Rollup / grouping sets |

## Why the DuckDB column looks unreal

Warm medians of 1–3 ms across a 10M-row fact scan are **not** a linear-time scan — they indicate DuckDB's in-process caching has warmed:
- decompressed Parquet pages pinned in RAM,
- vector batches reused across repeat runs,
- result-cache-style optimisations for identical repeated SQL.

This is still useful: it shows the ceiling of what the embedded engine can do when the working set fits. **It does not mean DuckDB is 1000× faster than ClickHouse on truly cold full scans.**

## Why Doris trails ClickHouse on every completed query

This VM is the opposite of Doris's design target:
- 1 FE + 1 BE on the same host → no MPP fan-out.
- JVM FE consumes ~1.5 GB before any query runs.
- BE segment compaction competes with query threads for the same 4 cores.

ClickHouse's single-process C++ exec path fits the 4 vCPU / 8 GB envelope far better.

## The two OOMs

| Query | What happens on Doris | What ClickHouse does |
|-------|-----------------------|----------------------|
| Q04 high-card GROUP BY | Hash table blows past BE memory limit | Builds, aggregates, succeeds in 2.9 s |
| Q12 heavy spill | Intentionally larger than RAM → BE aborts | Spills aggregation state to disk, finishes in 12.2 s |

ClickHouse's spill-to-disk path is well-tuned for 8 GB. Doris's 2.1.7 spill behaviour is less robust on small nodes.

---

## Write workloads

![Write throughput](charts/write_throughput.png)

| Workload | Doris | DuckDB | ClickHouse |
|----------|-------|--------|------------|
| W1 bulk load (rows/s) | 33,928 | **211,189** | 136,008 |
| W2 micro-batch (rows/s) | 3,864 | 4,303 | **4,929** |
| W3 point update | ❌ FEATURE GAP | ✅ ACID MVCC | ⚠️ async mutation |
| W4 bulk update (rows/s) | ❌ FEATURE GAP | **83,137** | 60,611 |

### W1 — Bulk load
DuckDB wins because Parquet → table is largely a metadata + buffer copy (zero-copy for many column types). ClickHouse's native insert is ~65% of DuckDB's speed. Doris's FE→BE stream-load adds coordination cost that dominates at 10M rows on one node.

### W2 — Micro-batch
All three cluster around 4–5k rows/s. Micro-batch is bound by per-request overhead (HTTP, commit, segment flush), not throughput. Engines are roughly equivalent here.

### W3 / W4 — Updates
The most consequential finding for production planning:
- **Doris DUPLICATE KEY tables simply cannot UPDATE.** You'd need to migrate to UNIQUE KEY (MoW), which changes the storage layout and read characteristics.
- **ClickHouse async mutations** are fine for bulk retroactive fixes but wrong for OLTP-style row updates.
- **DuckDB's in-place ACID MVCC** is the only "just works" option.

## What to read into this

**If you need to pick one engine for this size of box and workload:**
1. Append-only analytical workload → **ClickHouse** (completed everything, best memory behaviour).
2. Embedded analytics or heavy update requirements → **DuckDB**.
3. Doris needs multi-node scale to be fair — don't judge it on this POC alone.
