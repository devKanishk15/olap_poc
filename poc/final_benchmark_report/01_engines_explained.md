# Engines Explained — Doris, DuckDB, ClickHouse

A quick conceptual primer on what each engine is, how it executes queries, and why that shapes the numbers in this POC.

---

## Apache Doris 2.1.7

**What it is:** an MPP (massively-parallel-processing) analytical database with a frontend (FE, Java) and backend (BE, C++) separation. Designed for multi-node clusters where a single query fans out to many BEs in parallel.

### Architecture
- **FE (Frontend):** metadata, SQL parsing, planning, coordination. JVM-based.
- **BE (Backend):** columnar storage + vectorised execution. C++-based.
- **Protocol:** MySQL wire protocol — you can query it with any MySQL client.

### Storage models
| Model | Purpose | Updatable? |
|-------|---------|------------|
| `DUPLICATE KEY` | Append-only, fastest scans | ❌ no UPDATE |
| `UNIQUE KEY` (Merge-on-Write) | Row-level updates | ✅ MoW rewrites affected segments |
| `AGGREGATE KEY` | Pre-aggregated materialised data | partial — only on agg keys |

### Why it's slow locally on this POC
The whole point of Doris is to scale out. A single-node 1 FE + 1 BE setup on 8 GB RAM is the opposite of its design target. The FE JVM alone takes ~1.5 GB before any query runs.

### Why it wins on GCS
The `s3()` table-value function reads CSV parts in parallel threads. Network round-trips are the bottleneck, and Doris schedules the reads efficiently without over-buffering.

### Feature gaps that bit us
- `DUPLICATE KEY` tables (used for the fact table) **cannot be UPDATEd** → W3 & W4 return `FEATURE_GAP`.
- Q04 (high-card GROUP BY) and Q12 (heavy spill) OOM'd — BE segment merges + hash tables exceeded the 8 GB envelope.

---

## DuckDB 1.2.1

**What it is:** an analytical database that runs **in-process** as a library. No server, no daemon, no network. Think "SQLite, but columnar and vectorised."

### Architecture
- **Zero-copy Parquet reader:** Parquet pages are mapped and decoded directly into vector batches.
- **Push-based vectorised executor:** pipelines of 1024-tuple vectors flow through operators.
- **MVCC:** standard transactional semantics. Point and bulk UPDATE are ACID and in-place.

### Why it "dominates" locally
Every warm run in this POC is sub-3 ms. That's too fast to be "real" full-scan work on a 10M-row Parquet — DuckDB is actually caching:
1. the decompressed column vectors in memory, and
2. the query plan + result for identical repeat SQL.

So the reported "warm median" numbers reflect **best-case cache performance**, not a cold full scan. On a fresh dataset DuckDB would still be very fast, just not millisecond-fast.

### Why it loses on GCS
DuckDB uses the `httpfs` extension. For remote CSVs, each query re-downloads the file because DuckDB's remote read path isn't optimised for repeated-access caching on constrained RAM. Single-threaded HTTPS throughput on a 4 vCPU VM tops out ~13 s for the test CSV.

### Why it wins on updates
DuckDB is the only engine in this POC with a "normal" UPDATE story: full ACID, in-place, predicate pushdown, no async rewrite, no feature gap.

---

## ClickHouse 24.12

**What it is:** a column-oriented DBMS built around the **MergeTree** storage engine. Optimised for high-throughput append-only analytics and fast aggregations.

### Architecture
- **MergeTree parts:** every insert creates an immutable *part*. Background merges compact them.
- **Primary index (sparse):** not a B-tree — a coarse index that picks granules to read. Excellent for time-series/date-range scans.
- **Vectorised aggregation:** templated C++ code paths for common aggregate types.

### Mutations (aka UPDATE/DELETE)
ClickHouse does not do classic row-level UPDATE. `ALTER TABLE ... UPDATE` is an **async mutation**:
1. Submission is fast (~ms) — just registers the mutation.
2. Background workers rewrite every *part* touched by the predicate.
3. You must poll `system.mutations` to know when it's done.

This is fine for "batch fix 1% of rows once a day." It is the *wrong* tool for transactional per-row updates.

### Why it's the best all-rounder locally
- Completed **every** query, including Q12 (heavy spill) in 12 s.
- Handles memory pressure gracefully by spilling to disk — Doris OOM'd on the same queries.
- Aggregations (Q03, Q09, Q10) are 3–6× faster than Doris on this hardware.

### Why it's consistent on GCS
`s3()` TVF streams CSVs without aggressive caching — hence warm ≈ cold. Very predictable, ~7–8 s per query.

### Gotchas
- CSV int columns need explicit `Int64` in `s3()` schema to avoid overflow (patched in this POC).
- DDL files must strip trailing `--` comments and bare `;` before the harness appends `FORMAT JSON`.
- Multi-statement schema DDL requires `clickhouse-client --multiquery`, not HTTP.

---

## When would you choose which?

| Scenario | Pick |
|----------|------|
| Single analyst laptop, Parquet / CSV files, no ops | **DuckDB** |
| Server-grade but single-node warehouse, append-heavy logs | **ClickHouse** |
| Queries live over a cloud object store (GCS / S3) | **Doris** (on this POC) or ClickHouse |
| Transactional row-level updates inside the OLAP store | **DuckDB** only |
| Multi-tenant MPP cluster, SQL-dashboard workload at 10+ nodes | **Doris** (what it's designed for — this POC doesn't prove it) |
