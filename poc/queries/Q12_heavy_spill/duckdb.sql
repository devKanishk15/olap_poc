-- Q12 — Deliberately heavy query (spill candidate)
-- Dialect differences:
--   DuckDB uses APPROX_QUANTILE(col, 0.5) for percentiles (Arrow-based implementation)
--   DuckDB will auto-spill intermediates to /opt1/duckdb/spill
-- Dialect: DuckDB

WITH user_daily AS (
    SELECT
        user_id,
        event_date,
        device_type,
        country_code,
        SUM(revenue)                     AS day_revenue,
        SUM(order_total)                 AS day_order_total,
        COUNT(*)                         AS day_events,
        COUNT(DISTINCT session_id)       AS day_sessions,
        AVG(duration_ms)                 AS avg_duration
    FROM poc.event_fact
    WHERE is_bot = FALSE
    GROUP BY user_id, event_date, device_type, country_code
),
user_totals AS (
    SELECT
        user_id,
        device_type,
        country_code,
        SUM(day_revenue)                 AS total_revenue,
        SUM(day_order_total)             AS total_order_value,
        SUM(day_events)                  AS total_events,
        SUM(day_sessions)                AS total_sessions,
        COUNT(DISTINCT event_date)       AS active_days,
        AVG(avg_duration)                AS avg_session_duration
    FROM user_daily
    GROUP BY user_id, device_type, country_code
)
SELECT
    country_code,
    device_type,
    COUNT(DISTINCT user_id)              AS users_in_segment,
    AVG(total_revenue)                   AS avg_user_revenue,
    APPROX_QUANTILE(total_revenue, 0.5)  AS p50_revenue,
    APPROX_QUANTILE(total_revenue, 0.95) AS p95_revenue,
    AVG(active_days)                     AS avg_active_days,
    SUM(total_events)                    AS segment_events
FROM user_totals
GROUP BY country_code, device_type
ORDER BY avg_user_revenue DESC;
