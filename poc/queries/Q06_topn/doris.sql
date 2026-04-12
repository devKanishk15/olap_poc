-- Q06 — TOP-N with ORDER BY + LIMIT
-- Tests: partial sort / top-heap optimisation; engines should NOT sort all 10M rows
-- Expected: ClickHouse and DuckDB have optimised top-N; flag if sort takes full time
-- Dialect: Apache Doris

SELECT
    session_id,
    user_id,
    event_date,
    SUM(revenue)                         AS session_revenue,
    COUNT(*)                             AS event_count,
    MAX(order_total)                     AS max_order,
    MIN(event_ts)                        AS session_start
FROM poc.event_fact
WHERE
    event_date BETWEEN '2024-01-01' AND '2024-01-30'
    AND is_bot = FALSE
    AND revenue IS NOT NULL
GROUP BY session_id, user_id, event_date
ORDER BY session_revenue DESC
LIMIT 100;
