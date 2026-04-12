-- Q01 — Full table scan + aggregate
-- Dialect differences:
--   ClickHouse uses uniqExact() for exact distinct count (toCountDistinct is alias)
--   revenue is Decimal; ClickHouse AVG of Nullable INT is fine
-- Dialect: ClickHouse

SELECT
    count()                              AS total_events,
    sum(revenue)                         AS total_revenue,
    avg(duration_ms)                     AS avg_duration_ms,
    min(event_ts)                        AS earliest_event,
    max(event_ts)                        AS latest_event,
    uniqExact(user_id)                   AS distinct_users
FROM poc.event_fact;
