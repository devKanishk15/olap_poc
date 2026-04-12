-- Q12 — Deliberately heavy query (spill candidate)
-- Dialect differences:
--   ClickHouse: quantile(0.5)(col) is the percentile function syntax
--   uniqExact() for distinct counts; uniq() could be used for speed
--   max_bytes_before_external_group_by=3GB ensures spill rather than OOM
-- Dialect: ClickHouse

WITH user_daily AS (
    SELECT
        user_id,
        event_date,
        device_type,
        country_code,
        sum(revenue)                     AS day_revenue,
        sum(order_total)                 AS day_order_total,
        count()                          AS day_events,
        uniqExact(session_id)            AS day_sessions,
        avg(duration_ms)                 AS avg_duration
    FROM poc.event_fact
    WHERE is_bot = false
    GROUP BY user_id, event_date, device_type, country_code
),
user_totals AS (
    SELECT
        user_id,
        device_type,
        country_code,
        sum(day_revenue)                 AS total_revenue,
        sum(day_order_total)             AS total_order_value,
        sum(day_events)                  AS total_events,
        sum(day_sessions)                AS total_sessions,
        uniqExact(event_date)            AS active_days,
        avg(avg_duration)                AS avg_session_duration
    FROM user_daily
    GROUP BY user_id, device_type, country_code
)
SELECT
    country_code,
    device_type,
    uniqExact(user_id)                   AS users_in_segment,
    avg(total_revenue)                   AS avg_user_revenue,
    quantile(0.5)(total_revenue)         AS p50_revenue,
    quantile(0.95)(total_revenue)        AS p95_revenue,
    avg(active_days)                     AS avg_active_days,
    sum(total_events)                    AS segment_events
FROM user_totals
GROUP BY country_code, device_type
ORDER BY avg_user_revenue DESC
SETTINGS max_bytes_before_external_group_by = 3000000000;
