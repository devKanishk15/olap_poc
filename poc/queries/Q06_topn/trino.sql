-- Q06 — TOP-N with ORDER BY + LIMIT
-- Dialect: Trino 481

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
    event_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-30'
    AND is_bot = false
    AND revenue IS NOT NULL
GROUP BY session_id, user_id, event_date
ORDER BY session_revenue DESC
LIMIT 100
