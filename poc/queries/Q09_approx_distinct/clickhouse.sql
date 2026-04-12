-- Q09 — Approximate distinct count
-- Dialect differences:
--   ClickHouse: uniq() = HyperLogLog (~2.6% error), uniqHLL12() = explicit HLL
--   uniqExact() = exact (included for side-by-side comparison in same query)
--   This is ClickHouse's strongest card — uniq() is highly optimised
-- Dialect: ClickHouse

SELECT
    event_date,
    event_type,
    -- Approximate (HLL-based, fast)
    uniq(user_id)                        AS approx_distinct_users,
    uniq(session_id)                     AS approx_distinct_sessions,
    -- Exact (for comparison)
    uniqExact(device_id)                 AS exact_distinct_devices,
    count()                              AS total_events
FROM poc.event_fact
GROUP BY event_date, event_type
ORDER BY event_date, event_type;
