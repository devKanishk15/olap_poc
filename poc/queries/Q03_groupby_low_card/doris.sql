-- Q03 — GROUP BY low cardinality
-- Tests: aggregation over a column with ~20 distinct values (event_type)
-- Should be fast on all engines — fits in L2/L3 cache
-- Dialect: Apache Doris

SELECT
    event_type,
    COUNT(*)                             AS events,
    COUNT(DISTINCT user_id)              AS distinct_users,
    SUM(revenue)                         AS total_revenue,
    AVG(duration_ms)                     AS avg_duration_ms,
    SUM(CASE WHEN is_bot = TRUE THEN 1 ELSE 0 END) AS bot_events
FROM poc.event_fact
GROUP BY event_type
ORDER BY events DESC;
