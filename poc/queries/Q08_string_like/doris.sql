-- Q08 — String LIKE / regex pattern match
-- Tests: string scanning performance on high-cardinality varchar columns
-- Touches user_agent and referrer_url — both large strings, sparse in columnar storage
-- Dialect: Apache Doris

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
