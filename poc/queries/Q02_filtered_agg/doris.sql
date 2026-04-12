-- Q02 — Filtered aggregate (selective predicate)
-- Tests: predicate pushdown, partition pruning on event_date
-- Filter touches ~3.3% of rows (1 country + 1 event_type in a date range)
-- Dialect: Apache Doris

SELECT
    event_type,
    country_code,
    COUNT(*)                         AS events,
    SUM(revenue)                     AS total_revenue,
    AVG(order_total)                 AS avg_order_value,
    COUNT(DISTINCT user_id)          AS distinct_buyers
FROM poc.event_fact
WHERE
    event_date BETWEEN '2024-01-01' AND '2024-01-07'
    AND event_type = 'purchase'
    AND country_code IN ('US', 'GB', 'DE', 'FR', 'CA')
GROUP BY event_type, country_code
ORDER BY total_revenue DESC;
