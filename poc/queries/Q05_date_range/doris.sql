-- Q05 — Date-range scan (7-day window)
-- Tests: partition pruning efficiency; each engine should skip non-matching partitions
-- Only 7/30 partitions should be read — flag if full scan is observed
-- Dialect: Apache Doris

SELECT
    event_date,
    event_type,
    COUNT(*)                             AS events,
    SUM(revenue)                         AS daily_revenue,
    AVG(load_time_ms)                    AS avg_load_time_ms,
    COUNT(DISTINCT user_id)              AS active_users
FROM poc.event_fact
WHERE event_date BETWEEN '2024-01-08' AND '2024-01-14'
GROUP BY event_date, event_type
ORDER BY event_date ASC, events DESC;
