-- Q05 — Date-range scan (7-day window)
-- Dialect differences:
--   ClickHouse PARTITION BY toYYYYMM(event_date) — single partition covers Jan 2024
--   Granule-level skipping via sparse index on (event_date, event_type, user_id)
--   uniqExact() for distinct count
-- Dialect: ClickHouse

SELECT
    event_date,
    event_type,
    count()                              AS events,
    sum(revenue)                         AS daily_revenue,
    avg(load_time_ms)                    AS avg_load_time_ms,
    uniqExact(user_id)                   AS active_users
FROM poc.event_fact
WHERE event_date BETWEEN '2024-01-08' AND '2024-01-14'
GROUP BY event_date, event_type
ORDER BY event_date ASC, events DESC;
