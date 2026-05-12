-- Q08 — String LIKE / regex pattern match
-- Dialect: Trino 481
-- LIKE is standard; regexp_like() is Trino's regex function (Presto-compatible).

SELECT
    browser_family,
    os_family,
    COUNT(*)                             AS events,
    COUNT(DISTINCT user_id)              AS distinct_users,
    AVG(CAST(load_time_ms AS DOUBLE))    AS avg_load_ms
FROM poc.event_fact
WHERE
    (user_agent    LIKE '%Chrome/1%'
     OR user_agent LIKE '%Firefox/1%')
    AND referrer_url LIKE '%example.com%'
    AND is_bot = false
GROUP BY browser_family, os_family
ORDER BY events DESC
