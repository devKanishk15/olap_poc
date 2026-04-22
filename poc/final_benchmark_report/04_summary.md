# Summary — OLAP Engine POC

One-page recap. For details see [`02_local_analysis.md`](02_local_analysis.md) and [`03_gcs_analysis.md`](03_gcs_analysis.md).

## TL;DR

- **Local reads:** ClickHouse is the best real-world performer (completes every query; handles spill). DuckDB posts millisecond warm numbers but those are cache-warmed best cases.
- **GCS reads:** Doris wins by ~30% over ClickHouse; DuckDB is ~2× slower due to single-threaded httpfs.
- **Writes:** DuckDB is the only engine with standard ACID UPDATE semantics. Doris `DUPLICATE KEY` has no UPDATE at all; ClickHouse UPDATE is async.
- **Memory behaviour:** ClickHouse copes with spill on 8 GB RAM; Doris OOMs on Q04 and Q12.

## By-scenario recommendation

| Scenario | Pick | Reason |
|----------|------|--------|
| Analyst laptop, Parquet files | **DuckDB** | No ops, ACID updates, fastest local |
| Append-only single-node warehouse | **ClickHouse** | Completes everything, graceful spill |
| GCS / S3 query-in-place | **Apache Doris** | Lowest remote-read latency |
| Row-level updates | **DuckDB** | Only engine with in-place ACID MVCC |
| True MPP cluster (10+ nodes) | **Doris** | Designed for it — this POC can't measure it |

## Key numbers at a glance

### Local warm median (seconds, selected queries)
| Query | Doris | DuckDB | ClickHouse |
|-------|-------|--------|------------|
| Q01 full agg | 1.71 | 0.001 | 0.24 |
| Q07 join | 4.78 | 0.003 | 1.11 |
| Q09 approx distinct | 8.06 | 0.001 | 1.32 |
| Q12 heavy spill | 🌊 OOM | 0.002 | 12.24 |

### GCS warm median (seconds, selected queries)
| Query | Doris | DuckDB | ClickHouse |
|-------|-------|--------|------------|
| GQ01 full scan | **5.38** | 15.12 | 4.68 |
| GQ07 string like | **3.84** | 13.04 | 7.43 |
| GQ10 heavy scan | **4.77** | 12.63 | 7.85 |
| **Total (10 queries)** | **51.7 s** | 135.4 s | 72.8 s |

### Write throughput
| Workload | Doris | DuckDB | ClickHouse |
|----------|-------|--------|------------|
| W1 bulk load rows/s | 33,928 | **211,189** | 136,008 |
| W2 micro-batch rows/s | 3,864 | 4,303 | **4,929** |
| W3 point update | ❌ gap | ✅ ACID | ⚠️ async |
| W4 bulk update rows/s | ❌ gap | **83,137** | 60,611 |

## Caveats worth stating loudly

1. **Hardware constraint distorts Doris.** 4 vCPU / 8 GB RAM is the opposite of MPP's design target. Do not conclude Doris is "slow" — conclude Doris is miscast on one small box.
2. **DuckDB warm numbers are cached.** Sub-ms runs reflect hot-path caching; a truly cold scan would be higher (though still competitive).
3. **GCS numbers are network-bound.** Wall-clock results would change materially on a faster link or a VM closer to the bucket region.
4. **SQL semantics are matched across dialects**, documented in `schema/DIALECT_DIFFERENCES.md`.

## Where to go next

- Re-run this POC on 32 vCPU / 128 GB to see Doris close the local-read gap.
- Test Doris UNIQUE KEY (Merge-on-Write) tables to compare update semantics fairly.
- Add a materialised-view / rollup test so ClickHouse and Doris can flex their strengths.
