-- Q06 — TOP-N with ORDER BY + LIMIT
-- Dialect differences: None — DuckDB applies optimised top-N heap internally
-- Dialect: DuckDB

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
