# pc_item_image GCS Access Spec

Source: CSV file(s) in GCS bucket, ~70 GB total.  
All queries in `gcs_testing/queries/` access this table **directly from GCS** — no local load.

---

## Column Type Mapping

| Column | CSV (PostgreSQL) | Doris TVF | DuckDB `columns={}` | ClickHouse schema string |
|---|---|---|---|---|
| `pc_item_image_id` | numeric(10,0) | BIGINT | BIGINT | UInt64 |
| `fk_pc_item_id` | numeric(10,0) | BIGINT | BIGINT | UInt64 |
| `pc_item_image_original_width` | numeric(5,0) | INT | INTEGER | UInt32 |
| `pc_item_image_original_height` | numeric(5,0) | INT | INTEGER | UInt32 |
| `pc_item_image_125x125_width` | numeric(5,0) | INT | INTEGER | UInt32 |
| `pc_item_image_125x125_height` | numeric(5,0) | INT | INTEGER | UInt32 |
| `pc_item_image_250x250_width` | numeric(5,0) | INT | INTEGER | UInt32 |
| `pc_item_image_250x250_height` | numeric(5,0) | INT | INTEGER | UInt32 |
| `pc_item_image_500x500_width` | numeric(5,0) | INT | INTEGER | UInt32 |
| `pc_item_image_500x500_height` | numeric(5,0) | INT | INTEGER | UInt32 |
| `pc_item_image_original` | varchar(350) | VARCHAR(350) | VARCHAR | String |
| `pc_item_image_125x125` | varchar(350) | VARCHAR(350) | VARCHAR | String |
| `pc_item_image_250x250` | varchar(350) | VARCHAR(350) | VARCHAR | String |
| `pc_item_image_500x500` | varchar(350) | VARCHAR(350) | VARCHAR | String |
| `pc_item_image_accessed_by` | numeric(1,0) | INT | INTEGER | UInt8 |
| `pc_item_image_updatedby` | varchar(255) | VARCHAR(255) | VARCHAR | String |
| `pc_item_image_updatedby_id` | numeric(10,0) | BIGINT | BIGINT | UInt64 |
| `pc_item_image_updatescreen` | varchar(255) | VARCHAR(255) | VARCHAR | String |
| `pc_item_image_ip` | varchar(100) | VARCHAR(100) | VARCHAR | String |
| `pc_item_image_ip_country` | varchar(40) | VARCHAR(40) | VARCHAR | String |
| `pc_item_image_update_date` | timestamp | DATETIME | TIMESTAMP | DateTime |
| `pc_item_image_hist_comments` | varchar(1000) | VARCHAR(1000) | VARCHAR | String |
| `pc_item_image_updatedby_url` | varchar(255) | VARCHAR(255) | VARCHAR | String |
| `pc_item_image_updby_agency` | varchar(255) | VARCHAR(255) | VARCHAR | String |
| `pc_item_img_status` | char(1) | VARCHAR(1) | VARCHAR | String |
| `fk_pc_item_img_rejection_code` | numeric(5,0) | INT | INTEGER | UInt32 |
| `fk_pc_item_doc_id` | numeric(10,0) | BIGINT | BIGINT | UInt64 |
| `pc_item_img_doc_order` | numeric(10,0) | BIGINT | BIGINT | UInt64 |
| `pc_item_image_1000x1000` | varchar(350) | VARCHAR(350) | VARCHAR | String |
| `pc_item_image_1000x1000_width` | numeric(5,0) | INT | INTEGER | UInt32 |
| `pc_item_image_1000x1000_height` | numeric(5,0) | INT | INTEGER | UInt32 |
| `pc_item_image_glusr_id` | numeric(10,0) | BIGINT | BIGINT | UInt64 |
| `pc_item_image_2000x2000` | varchar(350) | VARCHAR(350) | VARCHAR | String |
| `pc_item_image_2000x2000_width` | numeric(5,0) | INT | INTEGER | UInt32 |
| `pc_item_image_2000x2000_height` | numeric(5,0) | INT | INTEGER | UInt32 |

Total: 35 columns.

---

## GCS Access Pattern per Engine

### Environment Variables

| Variable | Description |
|---|---|
| `GCS_BUCKET` | GCS bucket name (no `gs://` prefix) |
| `GCS_HMAC_ACCESS_KEY` | HMAC key ID (begins with `GOOG...`) |
| `GCS_HMAC_SECRET` | HMAC key secret |
| `GCS_PC_ITEM_IMAGE_PREFIX` | Key path within the bucket, e.g. `pc_feature/PC_ITEM_IMAGE.csv`. May include a glob pattern if multiple files, e.g. `pc_feature/PC_ITEM_IMAGE_*.csv` |
| `GCS_REGION` | `auto` (GCS does not use AWS regions, but the S3-compatible API accepts `auto`) |

### Apache Doris — `s3()` TVF

Doris 2.1+ supports explicit column declarations via the `"columns"` parameter. Required for queries referencing named columns because Doris CSV ingestion does not reliably infer column names from the header row at query time.

```sql
SELECT ...
FROM s3(
    "uri"              = "s3://<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>",
    "s3.endpoint"      = "https://storage.googleapis.com",
    "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
    "s3.secret_key"    = "<GCS_HMAC_SECRET>",
    "format"           = "csv",
    "column_separator" = ",",
    "columns"          = "pc_item_image_id BIGINT, fk_pc_item_id BIGINT, ..."
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
    's3://<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>',
    header = true,
    columns = {
        'pc_item_image_id': 'BIGINT',
        'fk_pc_item_id': 'BIGINT',
        -- ... all 35 columns
    }
)
```

**Notes:**
- The explicit `columns` map prevents `read_csv_auto` from mistyping `numeric(10,0)` columns as DOUBLE (auto-sampling uses only the first ~20k rows of a 70 GB file).
- `header = true` assumes the CSV has a header row.
- Spill to `/opt1/olap_poc/duckdb/spill` is expected for GQ04 and GQ10 on an 8 GB VM.

### ClickHouse — `s3()` Table Function

GQ01 uses `'CSVWithNames'` (all types become String; safe for count/uniqExact only).  
GQ02–GQ10 use `'CSV'` with an explicit schema string to get correct numeric types.

```sql
-- GQ01 (CSVWithNames — no schema string needed)
SELECT count(), uniqExact(fk_pc_item_id), ...
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>', '<GCS_HMAC_SECRET>',
    'CSVWithNames'
)

-- GQ02–GQ10 (CSV + schema string)
SELECT ...
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>', '<GCS_HMAC_SECRET>',
    'CSV',
    'pc_item_image_id UInt64, fk_pc_item_id UInt64, ...'
)
```

**Notes:**
- ClickHouse uses `https://` URI (not `s3://`) for GCS HMAC access.
- Do NOT end any file with `;` — the runner appends `FORMAT JSON`.
- Do NOT end any file with a bare `--` comment line — `_strip_sql()` removes trailing comment lines before appending `FORMAT JSON`, but a stray comment could silently consume the format clause in some edge cases.
- For GQ04 and GQ10, add `SETTINGS max_bytes_before_external_group_by = 3000000000` as the **last non-comment line** (not inside a comment, not followed by `;`).

---

## CSV Assumptions

- Header row: **present** (first line is column names matching the schema above).
- Delimiter: `,`
- Null representation: empty field or `\N` — all engines handle both with default settings.
- Timestamp format: `YYYY-MM-DD HH:MM:SS` (PostgreSQL default export).
- Encoding: UTF-8.

If the actual CSV deviates from these assumptions, update the TVF parameters accordingly and document the deviation here.

---

## Full Schema Strings (copy-paste ready)

### Doris `"columns"` parameter value
```
pc_item_image_id BIGINT, fk_pc_item_id BIGINT, pc_item_image_original_width INT, pc_item_image_original_height INT, pc_item_image_125x125_width INT, pc_item_image_125x125_height INT, pc_item_image_250x250_width INT, pc_item_image_250x250_height INT, pc_item_image_500x500_width INT, pc_item_image_500x500_height INT, pc_item_image_original VARCHAR(350), pc_item_image_125x125 VARCHAR(350), pc_item_image_250x250 VARCHAR(350), pc_item_image_500x500 VARCHAR(350), pc_item_image_accessed_by INT, pc_item_image_updatedby VARCHAR(255), pc_item_image_updatedby_id BIGINT, pc_item_image_updatescreen VARCHAR(255), pc_item_image_ip VARCHAR(100), pc_item_image_ip_country VARCHAR(40), pc_item_image_update_date DATETIME, pc_item_image_hist_comments VARCHAR(1000), pc_item_image_updatedby_url VARCHAR(255), pc_item_image_updby_agency VARCHAR(255), pc_item_img_status VARCHAR(1), fk_pc_item_img_rejection_code INT, fk_pc_item_doc_id BIGINT, pc_item_img_doc_order BIGINT, pc_item_image_1000x1000 VARCHAR(350), pc_item_image_1000x1000_width INT, pc_item_image_1000x1000_height INT, pc_item_image_glusr_id BIGINT, pc_item_image_2000x2000 VARCHAR(350), pc_item_image_2000x2000_width INT, pc_item_image_2000x2000_height INT
```

### ClickHouse schema string (5th argument to `s3()`)
```
pc_item_image_id UInt64, fk_pc_item_id UInt64, pc_item_image_original_width UInt32, pc_item_image_original_height UInt32, pc_item_image_125x125_width UInt32, pc_item_image_125x125_height UInt32, pc_item_image_250x250_width UInt32, pc_item_image_250x250_height UInt32, pc_item_image_500x500_width UInt32, pc_item_image_500x500_height UInt32, pc_item_image_original String, pc_item_image_125x125 String, pc_item_image_250x250 String, pc_item_image_500x500 String, pc_item_image_accessed_by UInt8, pc_item_image_updatedby String, pc_item_image_updatedby_id UInt64, pc_item_image_updatescreen String, pc_item_image_ip String, pc_item_image_ip_country String, pc_item_image_update_date DateTime, pc_item_image_hist_comments String, pc_item_image_updatedby_url String, pc_item_image_updby_agency String, pc_item_img_status String, fk_pc_item_img_rejection_code UInt32, fk_pc_item_doc_id UInt64, pc_item_img_doc_order UInt64, pc_item_image_1000x1000 String, pc_item_image_1000x1000_width UInt32, pc_item_image_1000x1000_height UInt32, pc_item_image_glusr_id UInt64, pc_item_image_2000x2000 String, pc_item_image_2000x2000_width UInt32, pc_item_image_2000x2000_height UInt32
```
