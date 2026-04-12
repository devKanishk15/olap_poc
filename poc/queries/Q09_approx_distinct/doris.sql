-- Q09 — Approximate distinct count (HLL / approx)
-- Tests: approximate COUNT(DISTINCT) — should be significantly faster than exact on 8 GB
-- Compare approx vs exact in results; document accuracy trade-off
-- Dialect: Apache Doris

SELECT
    event_date,
    event_type,
    -- Approximate distinct (HLL)
    APPROX_COUNT_DISTINCT(user_id)       AS approx_distinct_users,
    APPROX_COUNT_DISTINCT(session_id)    AS approx_distinct_sessions,
    -- Exact for comparison (may be slower)
    COUNT(DISTINCT device_id)            AS exact_distinct_devices,
    COUNT(*)                             AS total_events
FROM poc.event_fact
GROUP BY event_date, event_type
ORDER BY event_date, event_type;
