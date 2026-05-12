-- Q05 — Date-range scan (7-day window)
-- Dialect: Trino 481

SELECT
    event_date,
    event_type,
    COUNT(*)                             AS events,
    SUM(revenue)                         AS daily_revenue,
    AVG(CAST(load_time_ms AS DOUBLE))    AS avg_load_time_ms,
    COUNT(DISTINCT user_id)              AS active_users
FROM poc.event_fact
WHERE event_date BETWEEN DATE '2024-01-08' AND DATE '2024-01-14'
GROUP BY event_date, event_type
ORDER BY event_date ASC, events DESC
