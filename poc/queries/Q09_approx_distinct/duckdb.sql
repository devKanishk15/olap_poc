-- Q09 — Approximate distinct count
-- Dialect differences:
--   DuckDB uses APPROX_COUNT_DISTINCT() — HyperLogLog under the hood
--   approx_count_distinct is an alias in DuckDB
-- Dialect: DuckDB

SELECT
    event_date,
    event_type,
    APPROX_COUNT_DISTINCT(user_id)       AS approx_distinct_users,
    APPROX_COUNT_DISTINCT(session_id)    AS approx_distinct_sessions,
    COUNT(DISTINCT device_id)            AS exact_distinct_devices,
    COUNT(*)                             AS total_events
FROM poc.event_fact
GROUP BY event_date, event_type
ORDER BY event_date, event_type;
