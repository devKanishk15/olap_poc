# Canonical Wide Table Specification — `event_fact`

## Purpose

This document defines the single canonical table used across all three OLAP engines for the POC benchmarking. All three DDLs (`doris_ddl.sql`, `duckdb_ddl.sql`, `clickhouse_ddl.sql`) implement this exact logical schema using each engine's recommended types, storage layout, and indexing primitives.

---

## Table: `event_fact`

**Domain**: Web/app clickstream event fact table — a realistic wide fact table shape common in product analytics, adtech, and e-commerce.

**Row count**: 10,000,000 rows (10M)  
**Partition key**: `event_date` (30 days, ~333k rows/day)  
**Deterministic seed**: 42

---

## Column Definitions

| # | Column Name | Logical Type | Engine Type Notes | Description |
|---|-------------|-------------|-------------------|-------------|
| 1 | `event_id` | BIGINT (PK surrogate) | Auto or generated | Unique row identifier |
| 2 | `event_date` | DATE | DATE | Partition key — date part of event timestamp |
| 3 | `event_ts` | TIMESTAMP (μs) | DATETIME(6) / TIMESTAMPTZ / DateTime64(6) | Full event timestamp with microseconds |
| 4 | `session_id` | VARCHAR(36) | STRING / VARCHAR / String | UUID-format session identifier (high cardinality) |
| 5 | `user_id` | BIGINT | BIGINT | User identifier (medium cardinality, ~500k distinct) |
| 6 | `device_id` | VARCHAR(40) | STRING | Device fingerprint (high cardinality) |
| 7 | `event_type` | VARCHAR(30) | STRING | e.g. click, view, purchase, scroll (low cardinality, ~20 values) |
| 8 | `event_subtype` | VARCHAR(50) | STRING | Sub-classification of event (medium cardinality, ~200 values) |
| 9 | `page_id` | INT | INT | Page/screen identifier (~5,000 distinct) |
| 10 | `page_name` | VARCHAR(100) | STRING | Human-readable page name |
| 11 | `referrer_url` | VARCHAR(500) | STRING | HTTP referrer (nullable, high cardinality) |
| 12 | `campaign_id` | VARCHAR(50) | STRING | Marketing campaign tag (nullable, ~1,000 distinct) |
| 13 | `campaign_channel` | VARCHAR(30) | STRING | email / paid_search / organic / direct (low cardinality) |
| 14 | `ab_variant` | VARCHAR(10) | STRING | A/B test bucket A/B/C/control (very low cardinality) |
| 15 | `country_code` | CHAR(2) | STRING | ISO 3166-1 alpha-2 country (low cardinality, ~60 values) |
| 16 | `region` | VARCHAR(50) | STRING | State/province (medium cardinality) |
| 17 | `city` | VARCHAR(80) | STRING | City name (high cardinality) |
| 18 | `latitude` | DOUBLE | DOUBLE | Geo coordinate |
| 19 | `longitude` | DOUBLE | DOUBLE | Geo coordinate |
| 20 | `ip_address` | VARCHAR(45) | STRING | IPv4 or IPv6 address |
| 21 | `user_agent` | VARCHAR(300) | STRING | Raw browser UA string |
| 22 | `os_family` | VARCHAR(30) | STRING | Windows / macOS / iOS / Android / Linux (low cardinality) |
| 23 | `browser_family` | VARCHAR(30) | STRING | Chrome / Safari / Firefox / Edge (low cardinality) |
| 24 | `device_type` | VARCHAR(20) | STRING | desktop / mobile / tablet (very low cardinality) |
| 25 | `screen_width` | SMALLINT | SMALLINT / INT16 | Screen resolution width px |
| 26 | `screen_height` | SMALLINT | SMALLINT / INT16 | Screen resolution height px |
| 27 | `viewport_width` | SMALLINT | SMALLINT | Browser viewport width px |
| 28 | `viewport_height` | SMALLINT | SMALLINT | Browser viewport height px |
| 29 | `product_id` | INT | INT | Product SKU (nullable if non-product event; ~50k distinct) |
| 30 | `product_name` | VARCHAR(150) | STRING | Product display name (nullable) |
| 31 | `product_category_l1` | VARCHAR(60) | STRING | Top-level category (low cardinality, ~30 values) |
| 32 | `product_category_l2` | VARCHAR(80) | STRING | Sub-category (medium cardinality, ~300 values) |
| 33 | `product_price` | DECIMAL(12,2) | DECIMAL / NUMERIC | Product unit price (nullable) |
| 34 | `quantity` | SMALLINT | SMALLINT / INT16 | Items added/purchased (nullable, 1–20) |
| 35 | `order_id` | BIGINT | BIGINT | Order identifier (nullable, only for purchase events) |
| 36 | `order_total` | DECIMAL(14,2) | DECIMAL / NUMERIC | Order total value (nullable) |
| 37 | `discount_amount` | DECIMAL(10,2) | DECIMAL / NUMERIC | Discount applied (nullable) |
| 38 | `coupon_code` | VARCHAR(30) | STRING | Coupon code used (nullable, ~500 distinct) |
| 39 | `revenue` | DECIMAL(14,4) | DECIMAL (high precision) | Attributed revenue (can be fractional) |
| 40 | `duration_ms` | INT | INT | Time spent on page/interaction in milliseconds |
| 41 | `scroll_depth_pct` | TINYINT | TINYINT / INT8 | How far the user scrolled 0–100 |
| 42 | `click_x` | SMALLINT | SMALLINT | Click X coordinate on page |
| 43 | `click_y` | SMALLINT | SMALLINT | Click Y coordinate on page |
| 44 | `is_bot` | BOOLEAN | BOOLEAN | Bot detection flag |
| 45 | `is_authenticated` | BOOLEAN | BOOLEAN | User logged in at event time |
| 46 | `is_first_visit` | BOOLEAN | BOOLEAN | First session for this user |
| 47 | `experiment_id` | INT | INT | Active experiment ID (nullable) |
| 48 | `server_id` | SMALLINT | SMALLINT | Backend server that processed the event |
| 49 | `load_time_ms` | INT | INT | Page load time at event time (ms) |
| 50 | `ttfb_ms` | SMALLINT | SMALLINT | Time-to-first-byte in ms |
| 51 | `error_code` | SMALLINT | SMALLINT | HTTP or app error code (nullable, 0 = no error) |
| 52 | `error_message` | VARCHAR(255) | STRING | Error description (nullable) |
| 53 | `tag_list` | VARCHAR(500) | STRING (comma-sep) | Comma-separated content tags |
| 54 | `custom_dimensions` | JSON / MAP | JSON / Map(String,String) | Key-value pairs for custom event properties |
| 55 | `raw_payload_size_bytes` | INT | INT | Size of the original event payload |
| 56 | `ingestion_ts` | TIMESTAMP | DATETIME / DateTime64(3) | When the event was ingested into the pipeline |
| 57 | `processing_lag_ms` | INT | INT | Ingestion - event_ts in ms |
| 58 | `data_version` | TINYINT | TINYINT | Schema version tag (1–5) |
| 59 | `partition_key` | INT | INT | Synthetic partition hash (0–29) for non-date partitioning tests |
| 60 | `checksum` | BIGINT | BIGINT | CRC64 of select fields for integrity checks |

---

## Cardinality Summary (important for query design)

| Cardinality Level | Columns |
|-------------------|---------|
| Very Low (< 10) | `event_type`, `device_type`, `ab_variant`, `data_version` |
| Low (10–100) | `campaign_channel`, `country_code`, `os_family`, `browser_family`, `product_category_l1` |
| Medium (100–10k) | `event_subtype`, `region`, `page_id`, `campaign_id`, `product_category_l2`, `coupon_code` |
| High (10k–500k) | `user_id`, `product_id`, `product_name`, `city` |
| Very High (> 500k) | `event_id`, `session_id`, `device_id`, `order_id`, `user_agent`, `ip_address`, `referrer_url` |

---

## Partitioning and Sorting Strategy

| Engine | Partition | Sort / Cluster Key |
|--------|-----------|--------------------|
| Apache Doris | `PARTITION BY RANGE(event_date)` (monthly) | `DISTRIBUTED BY HASH(user_id)` |
| DuckDB | No explicit partitioning (file-level via Parquet partition) | No sort key (DuckDB handles this at scan time) |
| ClickHouse | `PARTITION BY toYYYYMM(event_date)` | `ORDER BY (event_date, event_type, user_id)` |

---

## Null Distribution

- ~15% of rows have `NULL` in `referrer_url`
- ~60% of rows have `NULL` in `order_id`, `order_total`, `discount_amount`, `coupon_code` (non-purchase events)
- ~30% of rows have `NULL` in `product_id`, `product_name`, `product_price`, `quantity`
- ~80% of rows have `NULL` in `error_code`, `error_message`
- ~20% of rows have `NULL` in `campaign_id`
