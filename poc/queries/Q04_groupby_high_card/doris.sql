-- Q04 — GROUP BY high cardinality
-- Tests: hash aggregation over ~500k distinct user_ids — likely spills on 8 GB
-- Flag this query if result takes > 120s or OOMs
-- Dialect: Apache Doris

SELECT
    user_id,
    COUNT(*)                             AS session_count,
    SUM(revenue)                         AS lifetime_revenue,
    AVG(duration_ms)                     AS avg_session_duration_ms,
    MAX(event_ts)                        AS last_seen,
    COUNT(DISTINCT session_id)           AS distinct_sessions,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count
FROM poc.event_fact
WHERE is_bot = FALSE
GROUP BY user_id
ORDER BY lifetime_revenue DESC
LIMIT 1000;
