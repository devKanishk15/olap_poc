-- Q09 — Approximate distinct count (HLL)
-- Dialect: Trino 481
-- Trino: approx_distinct(x) uses HyperLogLog (default ~2.3% error rate).
-- Dialect difference: APPROX_COUNT_DISTINCT(x) in Doris → approx_distinct(x) in Trino.

SELECT
    event_date,
    event_type,
    -- Approximate distinct (HLL) — Trino built-in
    approx_distinct(user_id)             AS approx_distinct_users,
    approx_distinct(session_id)          AS approx_distinct_sessions,
    -- Exact for comparison
    COUNT(DISTINCT device_id)            AS exact_distinct_devices,
    COUNT(*)                             AS total_events
FROM poc.event_fact
GROUP BY event_date, event_type
ORDER BY event_date, event_type
