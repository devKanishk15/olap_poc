-- Q06 — TOP-N with ORDER BY + LIMIT
-- Dialect differences:
--   ClickHouse has a dedicated top-N optimiser (max_bytes_before_external_sort for spill)
--   `isNotNull(revenue)` is the ClickHouse idiom for IS NOT NULL on Nullable columns
-- Dialect: ClickHouse

SELECT
    session_id,
    user_id,
    event_date,
    sum(revenue)                         AS session_revenue,
    count()                              AS event_count,
    max(order_total)                     AS max_order,
    min(event_ts)                        AS session_start
FROM poc.event_fact
WHERE
    event_date BETWEEN '2024-01-01' AND '2024-01-30'
    AND is_bot = false
    AND isNotNull(revenue)
GROUP BY session_id, user_id, event_date
ORDER BY session_revenue DESC
LIMIT 100;
