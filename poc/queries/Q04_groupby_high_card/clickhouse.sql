-- Q04 — GROUP BY high cardinality
-- Dialect differences:
--   uniqExact(session_id) instead of COUNT(DISTINCT session_id)
--   countIf(event_type = 'purchase') instead of SUM(CASE WHEN ...)
--   Note: max_bytes_before_external_group_by=3GB set at user level for spill
-- Dialect: ClickHouse

SELECT
    user_id,
    count()                              AS session_count,
    sum(revenue)                         AS lifetime_revenue,
    avg(duration_ms)                     AS avg_session_duration_ms,
    max(event_ts)                        AS last_seen,
    uniqExact(session_id)                AS distinct_sessions,
    countIf(event_type = 'purchase')     AS purchase_count
FROM poc.event_fact
WHERE is_bot = false
GROUP BY user_id
ORDER BY lifetime_revenue DESC
LIMIT 1000;
