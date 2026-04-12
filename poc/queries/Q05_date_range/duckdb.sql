-- Q05 — Date-range scan (7-day window)
-- Dialect differences:
--   DuckDB reads hive-partitioned Parquet; pruning is at file level (event_date= dirs)
--   COUNT(DISTINCT) uses HLL by default in DuckDB for large inputs
-- Dialect: DuckDB

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
