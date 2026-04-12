-- Q01 — Full table scan + aggregate
-- Dialect differences: None — identical semantics to Doris/ClickHouse
-- DuckDB uses TIMESTAMPTZ; COUNT(DISTINCT) uses HLL internally
-- Dialect: DuckDB

SELECT
    COUNT(*)                             AS total_events,
    SUM(revenue)                         AS total_revenue,
    AVG(duration_ms)                     AS avg_duration_ms,
    MIN(event_ts)                        AS earliest_event,
    MAX(event_ts)                        AS latest_event,
    COUNT(DISTINCT user_id)              AS distinct_users
FROM poc.event_fact;
