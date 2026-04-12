-- Q10 — Window function (running total + rank per partition)
-- Dialect differences: None — DuckDB has full ANSI window function support
-- Dialect: DuckDB

WITH daily_revenue AS (
    SELECT
        event_date,
        event_type,
        SUM(revenue)                     AS day_revenue,
        COUNT(DISTINCT user_id)          AS daily_users
    FROM poc.event_fact
    WHERE is_bot = FALSE
    GROUP BY event_date, event_type
)
SELECT
    event_date,
    event_type,
    day_revenue,
    daily_users,
    SUM(day_revenue) OVER (
        PARTITION BY event_type
        ORDER BY event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                    AS running_revenue,
    RANK() OVER (
        PARTITION BY event_type
        ORDER BY day_revenue DESC
    )                                    AS revenue_rank
FROM daily_revenue
ORDER BY event_type, event_date;
