-- Q08 — String LIKE / regex pattern match
-- Dialect differences: None — DuckDB supports LIKE natively
-- Dialect: DuckDB

SELECT
    browser_family,
    os_family,
    COUNT(*)                             AS events,
    COUNT(DISTINCT user_id)              AS distinct_users,
    AVG(load_time_ms)                    AS avg_load_ms
FROM poc.event_fact
WHERE
    (user_agent    LIKE '%Chrome/1%'
     OR user_agent LIKE '%Firefox/1%')
    AND referrer_url LIKE '%example.com%'
    AND is_bot = FALSE
GROUP BY browser_family, os_family
ORDER BY events DESC;
