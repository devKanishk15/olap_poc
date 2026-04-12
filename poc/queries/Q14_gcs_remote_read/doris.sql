-- Q14 — GCS remote read (external / table-function query)
-- Tests: query-time remote I/O throughput from GCS via S3-compatible API
-- This query is only run in --mode gcs; in local mode it is skipped
-- Semantics: same as Q01 (full scan + aggregate) but over GCS Parquet files
-- Dialect: Apache Doris (TVF — Table Value Function)
--
-- PREREQUISITES: GCS HMAC key configured in Doris catalog or session vars.
-- Replace <GCS_BUCKET> and <GCS_PREFIX> with values from .env

SELECT *
FROM s3(
    "uri"            = "s3://<GCS_BUCKET>/<GCS_PREFIX>",
    "s3.endpoint"    = "https://storage.googleapis.com",
    "s3.access_key"  = "${GCS_HMAC_ACCESS_KEY}",
    "s3.secret_key"  = "${GCS_HMAC_SECRET}",
    "format"         = "csv",
    "column_separator" = ","
)
LIMIT 100;

-- Alternatively, use an External Catalog if pre-configured:
-- SELECT COUNT(*), SUM(revenue) FROM gcs_catalog.poc.event_fact;
