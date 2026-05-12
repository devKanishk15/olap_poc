-- Q01 — Full table scan + aggregate
-- Dialect: Trino 481
-- COUNT(*), SUM, AVG, MIN, MAX, COUNT(DISTINCT) are standard SQL — identical to Doris.

SELECT
    COUNT(*)                             AS total_events,
    SUM(revenue)                         AS total_revenue,
    AVG(CAST(duration_ms AS DOUBLE))     AS avg_duration_ms,
    MIN(event_ts)                        AS earliest_event,
    MAX(event_ts)                        AS latest_event,
    COUNT(DISTINCT user_id)              AS distinct_users
FROM poc.event_fact
