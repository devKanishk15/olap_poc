-- Q08 — String LIKE / regex pattern match
-- Dialect differences:
--   ClickHouse: LIKE works; can also use `match()` (POSIX regex) for tighter testing
--   like(col, pattern) is equivalent to col LIKE pattern
--   Using match() variant here to stress regex engine vs LIKE on other engines
--   NOTE: mark this difference explicitly — semantics are identical, implementation differs
-- Dialect: ClickHouse

SELECT
    browser_family,
    os_family,
    count()                              AS events,
    uniqExact(user_id)                   AS distinct_users,
    avg(load_time_ms)                    AS avg_load_ms
FROM poc.event_fact
WHERE
    (match(user_agent, 'Chrome/1')
     OR match(user_agent, 'Firefox/1'))
    AND like(referrer_url, '%example.com%')
    AND is_bot = false
GROUP BY browser_family, os_family
ORDER BY events DESC;

-- Alternative using standard LIKE (uncomment to compare):
-- WHERE (user_agent LIKE '%Chrome/1%' OR user_agent LIKE '%Firefox/1%')
--   AND referrer_url LIKE '%example.com%'
