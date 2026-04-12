-- Q14 — GCS remote read (external / table-function query)
-- Dialect differences:
--   ClickHouse uses s3() table function with full HTTPS URL
--   GCS endpoint is storage.googleapis.com; HMAC keys used for authentication
-- Dialect: ClickHouse

SELECT
    count()                              AS total_events,
    sum(revenue)                         AS total_revenue,
    avg(duration_ms)                     AS avg_duration_ms,
    uniqExact(user_id)                   AS distinct_users,
    min(event_date)                      AS min_date,
    max(event_date)                      AS max_date
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_PREFIX>/event_fact/**/*.parquet',
    '<GCS_HMAC_ACCESS_KEY>',
    '<GCS_HMAC_SECRET>',
    'Parquet'
);

-- The harness substitutes credential placeholders at runtime.
-- ClickHouse also supports a named s3_cluster() for distributed reads (not used here).
