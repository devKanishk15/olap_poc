-- Q14 — GCS remote read
-- Dialect: Trino 481
-- Trino reads from the gcs_hive catalog (pre-configured with GCS HMAC credentials).
-- The runner connects to catalog=gcs_hive when mode=gcs, so this SQL is identical
-- to Q01 in structure — no TVF required. The catalog routes all I/O to GCS.

SELECT
    COUNT(*)                             AS total_events,
    SUM(revenue)                         AS total_revenue,
    AVG(CAST(duration_ms AS DOUBLE))     AS avg_duration_ms,
    COUNT(DISTINCT user_id)              AS distinct_users,
    MIN(event_date)                      AS min_date,
    MAX(event_date)                      AS max_date
FROM poc.event_fact
