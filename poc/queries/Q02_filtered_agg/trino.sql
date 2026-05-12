-- Q02 — Filtered aggregate (selective predicate)
-- Dialect: Trino 481

SELECT
    event_type,
    country_code,
    COUNT(*)                         AS events,
    SUM(revenue)                     AS total_revenue,
    AVG(order_total)                 AS avg_order_value,
    COUNT(DISTINCT user_id)          AS distinct_buyers
FROM poc.event_fact
WHERE
    event_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-07'
    AND event_type = 'purchase'
    AND country_code IN ('US', 'GB', 'DE', 'FR', 'CA')
GROUP BY event_type, country_code
ORDER BY total_revenue DESC
