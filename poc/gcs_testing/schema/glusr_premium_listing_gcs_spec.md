# glusr_premium_listing GCS Access Spec

Source: CSV file in GCS bucket — `gs://pc_feature/GLUSR_PREMIUM_LISTING.csv`  
All queries in `gcs_testing/queries/` access this table **directly from GCS** — no local load.

---

## Column Type Mapping

| Column | Description | Doris TVF | DuckDB `columns={}` | ClickHouse schema string |
|---|---|---|---|---|
| `glusr_premium_listing_id` | Unique listing record ID | BIGINT | BIGINT | UInt64 |
| `fk_glusr_usr_id` | User (seller/buyer) reference | BIGINT | BIGINT | UInt64 |
| `glusr_premium_mcat_id` | Master category mapping | BIGINT | BIGINT | UInt64 |
| `glusr_premium_city_id` | City/location ID | BIGINT | BIGINT | UInt64 |
| `flag_premium_listing` | Premium/special listing flag | VARCHAR(10) | VARCHAR | String |
| `fk_service_id` | Service reference | BIGINT | BIGINT | UInt64 |
| `fk_cust_to_serv_id` | Customer-to-service mapping | BIGINT | BIGINT | UInt64 |
| `pl_kwrd_term_upper` | Keyword terms (uppercase) for search | VARCHAR(500) | VARCHAR | String |
| `glusr_premium_enable` | Active/enabled flag | VARCHAR(10) | VARCHAR | String |
| `glusr_premium_added_date` | Record creation timestamp | DATETIME | TIMESTAMP | DateTime |
| `last_modified_date` | Last update timestamp | DATETIME | TIMESTAMP | DateTime |
| `glusr_premium_updatedby_id` | ID of last updater | BIGINT | BIGINT | UInt64 |
| `glusr_premium_updatedby` | Name/identifier of updater | VARCHAR(255) | VARCHAR | String |
| `glusr_premium_updatescreen` | Screen/interface of last update | VARCHAR(255) | VARCHAR | String |
| `glusr_premium_ip` | IP address of last modifier | VARCHAR(100) | VARCHAR | String |
| `glusr_premium_ip_country` | Country derived from IP | VARCHAR(40) | VARCHAR | String |
| `glusr_premium_hist_comments` | Historical comments/remarks | VARCHAR(1000) | VARCHAR | String |
| `glusr_premium_updatedby_url` | URL related to updater/action | VARCHAR(255) | VARCHAR | String |
| `category_type` | Category classification (B2B, B2C…) | VARCHAR(50) | VARCHAR | String |
| `location_type` | Location granularity (city/state/country) | VARCHAR(50) | VARCHAR | String |
| `location_iso` | ISO country code | VARCHAR(10) | VARCHAR | String |
| `category_location_credit_value` | Credits/weight for category-location combo | DOUBLE | DOUBLE | Float64 |

Total: 22 columns.

---

## GCS Access Pattern per Engine

### Environment Variables

| Variable | Description |
|---|---|
| `GCS_BUCKET` | GCS bucket name (no `gs://` prefix), e.g. `pc_feature` |
| `GCS_HMAC_ACCESS_KEY` | HMAC key ID (begins with `GOOG...`) |
| `GCS_HMAC_SECRET` | HMAC key secret |
| `GCS_GLUSR_PREMIUM_LISTING_PREFIX` | Key path within the bucket, e.g. `pc_feature/GLUSR_PREMIUM_LISTING.csv` |
| `GCS_REGION` | `auto` (GCS does not use AWS regions, but the S3-compatible API accepts `auto`) |

### Apache Doris — `s3()` TVF

Doris 2.1+ supports explicit column declarations via the `"columns"` parameter. Required for queries referencing named columns because Doris CSV ingestion does not reliably infer column names from the header row at query time.

```sql
SELECT ...
FROM s3(
    "uri"              = "s3://<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>",
    "s3.endpoint"      = "https://storage.googleapis.com",
    "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
    "s3.secret_key"    = "<GCS_HMAC_SECRET>",
    "format"           = "csv",
    "column_separator" = ",",
    "columns"          = "glusr_premium_listing_id BIGINT, fk_glusr_usr_id BIGINT, ..."
)
```

**Notes:**
- URI uses `s3://` scheme; the `s3.endpoint` redirects to GCS.
- Do NOT end the file with a bare `--` comment line.
- Do NOT add a trailing `;` (the mysql-connector sends single statements).
- Connect without `database=` parameter — TVF queries are database-agnostic.

### DuckDB — `read_csv_auto()`

The runner sets S3 credentials via `con.execute()` calls before executing the SQL file. The SQL file contains only the SELECT statement.

```sql
-- Runner injects these before executing this file:
--   LOAD httpfs;
--   SET s3_endpoint = 'storage.googleapis.com';
--   SET s3_access_key_id = '<GCS_HMAC_ACCESS_KEY>';
--   SET s3_secret_access_key = '<GCS_HMAC_SECRET>';
--   SET s3_region = 'auto';
--   SET memory_limit = '6GB';
--   SET temp_directory = '/opt1/olap_poc/duckdb/spill';

SELECT ...
FROM read_csv_auto(
    's3://<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    header = true,
    columns = {
        'glusr_premium_listing_id': 'BIGINT',
        'fk_glusr_usr_id': 'BIGINT',
        -- ... all 22 columns
    }
)
```

**Notes:**
- The explicit `columns` map prevents `read_csv_auto` from mistyping numeric ID columns as DOUBLE.
- `header = true` assumes the CSV has a header row.
- `GCS_GLUSR_PREMIUM_LISTING_PREFIX` must include the bucket name as its first path component, e.g. `pc_feature/GLUSR_PREMIUM_LISTING.csv`.

### ClickHouse — `s3()` Table Function

GQ01 uses `'CSVWithNames'` (all types become String; safe for count/uniqExact only).  
GQ02–GQ10 use `'CSV'` with an explicit schema string to get correct numeric types.

```sql
-- GQ01 (CSVWithNames — no schema string needed)
SELECT count(), uniqExact(fk_glusr_usr_id), ...
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>', '<GCS_HMAC_SECRET>',
    'CSVWithNames'
)

-- GQ02–GQ10 (CSV + schema string)
SELECT ...
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>', '<GCS_HMAC_SECRET>',
    'CSV',
    'glusr_premium_listing_id UInt64, fk_glusr_usr_id UInt64, ...'
)
```

**Notes:**
- ClickHouse uses `https://` URI (not `s3://`) for GCS HMAC access.
- Do NOT end any file with `;` — the runner appends `FORMAT JSON`.
- Do NOT end any file with a bare `--` comment line.
- For GQ04 and GQ10, add `SETTINGS max_bytes_before_external_group_by = 3000000000` as the **last non-comment line**.

---

## CSV Assumptions

- Header row: **present** (first line is column names matching the schema above).
- Delimiter: `,`
- Null representation: empty field or `\N` — all engines handle both with default settings.
- Timestamp format: `YYYY-MM-DD HH:MM:SS` (PostgreSQL default export).
- Encoding: UTF-8.

---

## Full Schema Strings (copy-paste ready)

### Doris `"columns"` parameter value
```
glusr_premium_listing_id BIGINT, fk_glusr_usr_id BIGINT, glusr_premium_mcat_id BIGINT, glusr_premium_city_id BIGINT, flag_premium_listing VARCHAR(10), fk_service_id BIGINT, fk_cust_to_serv_id BIGINT, pl_kwrd_term_upper VARCHAR(500), glusr_premium_enable VARCHAR(10), glusr_premium_added_date DATETIME, last_modified_date DATETIME, glusr_premium_updatedby_id BIGINT, glusr_premium_updatedby VARCHAR(255), glusr_premium_updatescreen VARCHAR(255), glusr_premium_ip VARCHAR(100), glusr_premium_ip_country VARCHAR(40), glusr_premium_hist_comments VARCHAR(1000), glusr_premium_updatedby_url VARCHAR(255), category_type VARCHAR(50), location_type VARCHAR(50), location_iso VARCHAR(10), category_location_credit_value DOUBLE
```

### ClickHouse schema string (5th argument to `s3()`)
```
glusr_premium_listing_id UInt64, fk_glusr_usr_id UInt64, glusr_premium_mcat_id UInt64, glusr_premium_city_id UInt64, flag_premium_listing String, fk_service_id UInt64, fk_cust_to_serv_id UInt64, pl_kwrd_term_upper String, glusr_premium_enable String, glusr_premium_added_date DateTime, last_modified_date DateTime, glusr_premium_updatedby_id UInt64, glusr_premium_updatedby String, glusr_premium_updatescreen String, glusr_premium_ip String, glusr_premium_ip_country String, glusr_premium_hist_comments String, glusr_premium_updatedby_url String, category_type String, location_type String, location_iso String, category_location_credit_value Float64
```
