-- Q03 — GROUP BY low cardinality
-- Dialect: Trino 481

SELECT
    event_type,
    COUNT(*)                             AS events,
    COUNT(DISTINCT user_id)              AS distinct_users,
    SUM(revenue)                         AS total_revenue,
    AVG(CAST(duration_ms AS DOUBLE))     AS avg_duration_ms,
    SUM(CASE WHEN is_bot = true THEN 1 ELSE 0 END) AS bot_events
FROM poc.event_fact
GROUP BY event_type
ORDER BY events DESC
