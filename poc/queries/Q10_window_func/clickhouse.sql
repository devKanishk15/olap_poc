-- Q10 — Window function (running total + rank per partition)
-- Dialect differences:
--   ClickHouse supports window functions since v21.3 (stable in 24.x)
--   uniqExact() in CTE for distinct count; rest of window syntax is ANSI-compatible
--   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW is supported
-- Dialect: ClickHouse

WITH daily_revenue AS (
    SELECT
        event_date,
        event_type,
        sum(revenue)                     AS day_revenue,
        uniqExact(user_id)               AS daily_users
    FROM poc.event_fact
    WHERE is_bot = false
    GROUP BY event_date, event_type
)
SELECT
    event_date,
    event_type,
    day_revenue,
    daily_users,
    sum(day_revenue) OVER (
        PARTITION BY event_type
        ORDER BY event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                    AS running_revenue,
    rank() OVER (
        PARTITION BY event_type
        ORDER BY day_revenue DESC
    )                                    AS revenue_rank
FROM daily_revenue
ORDER BY event_type, event_date;
