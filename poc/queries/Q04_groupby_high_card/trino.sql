-- Q04 — GROUP BY high cardinality
-- Dialect: Trino 481

SELECT
    user_id,
    COUNT(*)                             AS session_count,
    SUM(revenue)                         AS lifetime_revenue,
    AVG(CAST(duration_ms AS DOUBLE))     AS avg_session_duration_ms,
    MAX(event_ts)                        AS last_seen,
    COUNT(DISTINCT session_id)           AS distinct_sessions,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count
FROM poc.event_fact
WHERE is_bot = false
GROUP BY user_id
ORDER BY lifetime_revenue DESC
LIMIT 1000
