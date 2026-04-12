-- Q14 — GCS remote read (external / table-function query)
-- Dialect differences:
--   DuckDB uses read_parquet() with s3:// URI and SET s3_* session variables
--   httpfs extension must be loaded (done in startup.sql)
-- Dialect: DuckDB
--
-- Run after: SET s3_endpoint='storage.googleapis.com'; SET s3_access_key_id='...'; etc.

SELECT
    COUNT(*)                             AS total_events,
    SUM(revenue)                         AS total_revenue,
    AVG(duration_ms)                     AS avg_duration_ms,
    COUNT(DISTINCT user_id)              AS distinct_users,
    MIN(event_date)                      AS min_date,
    MAX(event_date)                      AS max_date
FROM read_parquet(
    's3://<GCS_BUCKET>/<GCS_PREFIX>/event_fact/**/*.parquet',
    hive_partitioning = true
);

-- The harness injects s3_* credentials as SET statements before running this query.
