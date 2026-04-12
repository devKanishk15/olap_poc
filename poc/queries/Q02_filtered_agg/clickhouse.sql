-- Q02 — Filtered aggregate (selective predicate)
-- Dialect differences:
--   ClickHouse uses uniqExact() for distinct count
--   BETWEEN is supported; IN() works the same way
-- Dialect: ClickHouse

SELECT
    event_type,
    country_code,
    count()                          AS events,
    sum(revenue)                     AS total_revenue,
    avg(order_total)                 AS avg_order_value,
    uniqExact(user_id)               AS distinct_buyers
FROM poc.event_fact
WHERE
    event_date BETWEEN '2024-01-01' AND '2024-01-07'
    AND event_type = 'purchase'
    AND country_code IN ('US', 'GB', 'DE', 'FR', 'CA')
GROUP BY event_type, country_code
ORDER BY total_revenue DESC;
