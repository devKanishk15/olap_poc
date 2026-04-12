-- Q03 — GROUP BY low cardinality
-- Dialect differences:
--   uniqExact() instead of COUNT(DISTINCT)
--   countIf() instead of SUM(CASE WHEN ...)  — ClickHouse idiom; functionally identical
-- Dialect: ClickHouse

SELECT
    event_type,
    count()                              AS events,
    uniqExact(user_id)                   AS distinct_users,
    sum(revenue)                         AS total_revenue,
    avg(duration_ms)                     AS avg_duration_ms,
    countIf(is_bot = true)               AS bot_events
FROM poc.event_fact
GROUP BY event_type
ORDER BY events DESC;
