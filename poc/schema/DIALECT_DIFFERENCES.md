# SQL Dialect Differences — Cross-Engine Reference

This document is the canonical reference for every SQL dialect difference across
**Apache Doris**, **DuckDB**, and **ClickHouse** as used in this POC.
It is intended to be read alongside `report/04_final_report.md` Section 10C.

---

## 1. Aggregate Functions

| Operation | Doris | DuckDB | ClickHouse | Notes |
|-----------|-------|--------|-----------|-------|
| Exact distinct count | `COUNT(DISTINCT x)` | `COUNT(DISTINCT x)` | `uniqExact(x)` | CH `COUNT(DISTINCT x)` aliases `uniqExact` |
| Approximate distinct | `APPROX_COUNT_DISTINCT(x)` | `APPROX_COUNT_DISTINCT(x)` | `uniq(x)` | CH `uniq()` = HyperLogLog (~2.6% error) |
| HLL explicit | `HLL_UNION_AGG(HLL_HASH(x))` | n/a | `uniqHLL12(x)` | Doris has native HLL type; DuckDB uses APPROX |
| Percentile (approx) | `PERCENTILE_APPROX(x, 0.95)` | `APPROX_QUANTILE(x, 0.95)` | `quantile(0.95)(x)` | CH uses curried syntax for all quantile funcs |
| Percentile (exact) | `PERCENTILE(x, 0.95)` | `QUANTILE(x, 0.95)` | `quantileExact(0.95)(x)` | Expensive on large datasets |
| Conditional count | `SUM(CASE WHEN c THEN 1 ELSE 0 END)` | `SUM(CASE WHEN c THEN 1 ELSE 0 END)` | `countIf(c)` | `countIf` is idiomatic CH; CASE WHEN also works |
| Conditional sum | `SUM(CASE WHEN c THEN x ELSE 0 END)` | `SUM(CASE WHEN c THEN x ELSE 0 END)` | `sumIf(x, c)` | `sumIf` is idiomatic CH |
| Standard COUNT(*) | `COUNT(*)` | `COUNT(*)` | `count()` | CH `count()` is equivalent |
| Standard SUM | `SUM(x)` | `SUM(x)` | `sum(x)` | CH function names are lowercase by convention |

---

## 2. NULL Handling

| Operation | Doris | DuckDB | ClickHouse | Notes |
|-----------|-------|--------|-----------|-------|
| NULL check | `x IS NULL` | `x IS NULL` | `isNull(x)` or `x IS NULL` | Both work in CH; `isNull()` is preferred idiom |
| NOT NULL check | `x IS NOT NULL` | `x IS NOT NULL` | `isNotNull(x)` | See Q06 usage |
| NULL coalesce | `COALESCE(x, default)` | `COALESCE(x, default)` | `coalesce(x, default)` | Identical |
| NULL guard divisor | `NULLIF(x, 0)` | `NULLIF(x, 0)` | `nullIf(x, 0)` | CH uses camelCase `nullIf` |
| NULL-safe filter | `WHERE x IS NOT NULL` | `WHERE x IS NOT NULL` | `WHERE isNotNull(x)` | Q06, Q09 |

---

## 3. String Functions

| Operation | Doris | DuckDB | ClickHouse | Notes |
|-----------|-------|--------|-----------|-------|
| Pattern match | `col LIKE '%pat%'` | `col LIKE '%pat%'` | `like(col, '%pat%')` or `col LIKE '%pat%'` | Standard LIKE works in all; CH has function form |
| Regex match | `col REGEXP 'pattern'` | `regexp_matches(col, 'pattern')` | `match(col, 'pattern')` | Q08: semantics equivalent, implementation differs |
| Lowercase | `LOWER(x)` | `LOWER(x)` | `lower(x)` / `lowerUTF8(x)` | CH has UTF-8 aware variant |
| Uppercase | `UPPER(x)` | `UPPER(x)` | `upper(x)` / `upperUTF8(x)` | |
| Substring | `SUBSTRING(s, pos, len)` | `SUBSTRING(s, pos, len)` | `substring(s, pos, len)` | Identical |
| String length | `LENGTH(s)` | `LENGTH(s)` | `length(s)` / `lengthUTF8(s)` | |
| Concat | `CONCAT(a, b)` | `CONCAT(a, b)` or `a \|\| b` | `concat(a, b)` | All work |

---

## 4. JSON / Semi-Structured

| Operation | Doris | DuckDB | ClickHouse | Notes |
|-----------|-------|--------|-----------|-------|
| Extract string by key | `JSON_EXTRACT_STRING(col, '$.key')` | `col ->> '$.key'` or `json_extract_string(col, '$.key')` | `JSONExtractString(col, 'key')` | CH drops `$.` prefix — key name only |
| Extract integer | `JSON_EXTRACT_INT(col, '$.key')` | `(col ->> '$.key')::INT` | `JSONExtractInt(col, 'key')` | |
| Check key exists | `JSON_CONTAINS_PATH(col, '$.key')` | `json_contains(col, '"key"')` | `JSONHas(col, 'key')` | |
| Column type | `JSON` (native type) | `JSON` (native type) | `String` (JSON stored as text) | CH 24.x has experimental `JSON` type; String is stable |
| NULL empty check | `col IS NOT NULL` | `col IS NOT NULL` | `col != ''` | CH String col: empty string ≠ NULL; check Q11 |

---

## 5. Date / Time Functions

| Operation | Doris | DuckDB | ClickHouse | Notes |
|-----------|-------|--------|-----------|-------|
| Current date | `CURDATE()` / `CURRENT_DATE` | `CURRENT_DATE` | `today()` | |
| Current timestamp | `NOW()` / `CURRENT_TIMESTAMP` | `NOW()` | `now()` | |
| Extract year | `YEAR(col)` | `YEAR(col)` | `toYear(col)` | |
| Extract month | `MONTH(col)` | `MONTH(col)` | `toMonth(col)` | |
| Extract day | `DAY(col)` | `DAY(col)` | `toDayOfMonth(col)` | |
| YYYYmm integer | `DATE_FORMAT(col, '%Y%m')` | `strftime(col, '%Y%m')` | `toYYYYMM(col)` | Used in CH PARTITION BY |
| Date truncate | `DATE_TRUNC('month', col)` | `DATE_TRUNC('month', col)` | `toStartOfMonth(col)` | |
| Add interval | `DATE_ADD(col, INTERVAL n DAY)` | `col + INTERVAL n DAY` | `col + toIntervalDay(n)` | |
| BETWEEN | `col BETWEEN a AND b` | `col BETWEEN a AND b` | `col BETWEEN a AND b` | Identical |

---

## 6. Window Functions

ClickHouse added full window function support in **v21.3** (stable in 24.x).

| Operation | Doris | DuckDB | ClickHouse | Notes |
|-----------|-------|--------|-----------|-------|
| Running total | `SUM(x) OVER (PARTITION BY p ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` | Same | Same | Fully ANSI — identical in all three |
| Rank | `RANK() OVER (PARTITION BY p ORDER BY o DESC)` | Same | Same | Identical |
| Row number | `ROW_NUMBER() OVER (...)` | Same | Same | Identical |
| Lead/Lag | `LAG(col, 1) OVER (...)` | Same | Same | Identical |
| Frame syntax | `ROWS BETWEEN ...` | `ROWS BETWEEN ...` | `ROWS BETWEEN ...` | Identical |

---

## 7. GCS / Remote Storage Access

| Feature | Doris | DuckDB | ClickHouse |
|---------|-------|--------|-----------|
| Function | `s3("uri", ..., "parquet")` TVF | `read_parquet('s3://...')` | `s3('https://...', key, secret, 'Parquet')` |
| URL scheme | `s3://bucket/path` | `s3://bucket/path` | `https://storage.googleapis.com/bucket/path` |
| Auth method | HMAC key in TVF args | `SET s3_access_key_id / s3_secret_access_key` | HMAC key in function args |
| Glob support | `**/*.parquet` | `**/*.parquet` | `**/*.parquet` |
| Hive partitioning | Via External Catalog | `hive_partitioning=true` in read_parquet | Not natively; flatten glob |
| Write to GCS | Via EXPORT TO | `COPY TO 's3://...'` | `INSERT INTO FUNCTION s3(...)` |

---

## 8. UPDATE / DELETE Semantics

This is the **most significant** cross-engine semantic difference in the POC.

| Feature | Doris | DuckDB | ClickHouse |
|---------|-------|--------|-----------|
| Point UPDATE | ✅ (Unique Key MoW only) | ✅ (MVCC) | ⚠️ Async mutation |
| Bulk UPDATE | ✅ (Unique Key MoW only) | ✅ (MVCC) | ⚠️ Async mutation |
| UPDATE on Duplicate Key | ❌ Not supported | N/A | N/A |
| DELETE | ✅ (MoW) | ✅ | ⚠️ Async mutation |
| UPDATE latency | ~ms (MoW) | ~ms | Submission ~ms, completion seconds–minutes |
| ACID | ✅ (MoW) | ✅ | ❌ |
| Recommended pattern | Unique Key MoW for mutability; Duplicate Key for read-only | Standard UPDATE | ReplacingMergeTree + periodic OPTIMIZE |

---

## 9. Data Type Mapping

| Logical Type | Doris | DuckDB | ClickHouse |
|-------------|-------|--------|-----------|
| Signed 8-bit int | `TINYINT` | `TINYINT` | `Int8` |
| Signed 16-bit int | `SMALLINT` | `SMALLINT` | `Int16` |
| Signed 32-bit int | `INT` | `INTEGER` | `Int32` |
| Signed 64-bit int | `BIGINT` | `BIGINT` | `Int64` |
| 32-bit float | `FLOAT` | `FLOAT` | `Float32` |
| 64-bit float | `DOUBLE` | `DOUBLE` | `Float64` |
| Fixed decimal | `DECIMAL(p,s)` | `DECIMAL(p,s)` | `Decimal(p,s)` |
| Variable string | `VARCHAR(n)` | `VARCHAR(n)` | `String` (no length limit) |
| Fixed char | `CHAR(n)` | `VARCHAR(n)` (aliased) | `FixedString(n)` |
| Date only | `DATE` | `DATE` | `Date` |
| Datetime ms | `DATETIME(3)` | `TIMESTAMP` | `DateTime64(3)` |
| Datetime µs | `DATETIME(6)` | `TIMESTAMPTZ` | `DateTime64(6, 'UTC')` |
| Boolean | `BOOLEAN` | `BOOLEAN` | `Bool` |
| JSON | `JSON` | `JSON` | `String` (stable) |
| Low-cardinality hint | N/A | N/A | `LowCardinality(T)` |
| Nullable wrapper | `NULL` column prop | `NULL` column prop | `Nullable(T)` |

---

## 10. Feature Gap Summary Table

| Feature | Doris | DuckDB | ClickHouse |
|---------|:-----:|:------:|:----------:|
| Standard SQL UPDATE (Duplicate Key) | ❌ | ✅ | ⚠️ |
| ACID transactions | ⚠️ | ✅ | ❌ |
| Server / multi-user mode | ✅ | ❌ | ✅ |
| Horizontal scale-out | ✅ | ❌ | ✅ |
| Native partitioning (DDL) | ✅ | ❌ | ✅ |
| Materialized views | ✅ | ❌ | ✅ |
| GCS write via SQL | ✅ | ✅ | ✅ |
| MySQL-compatible wire protocol | ✅ | ❌ | ⚠️ |
| LowCardinality encoding | ❌ | ❌ | ✅ |
| Embedded / library mode | ❌ | ✅ | ❌ |
| JSON native type (stable) | ✅ | ✅ | ❌ (experimental) |
| Approximate COUNT DISTINCT | ✅ | ✅ | ✅ |
| Window functions (full ANSI) | ✅ | ✅ | ✅ (v21.3+) |

✅ = supported  ⚠️ = partial / caveated  ❌ = not supported / gap
