-- Q11 — JSON field extraction
-- Dialect differences:
--   ClickHouse: custom_dimensions stored as String (JSON text)
--   Use JSONExtractString() for key extraction — standard function in all CH versions
--   simpleJSONExtractString() is faster but less spec-compliant; JSONExtractString preferred
-- Dialect: ClickHouse

SELECT
    JSONExtractString(custom_dimensions, 'plan')     AS plan_tier,
    JSONExtractString(custom_dimensions, 'theme')    AS ui_theme,
    event_type,
    count()                                          AS events,
    sum(revenue)                                     AS total_revenue,
    avg(duration_ms)                                 AS avg_duration_ms
FROM poc.event_fact
WHERE
    custom_dimensions != ''
    AND event_date BETWEEN '2024-01-01' AND '2024-01-30'
GROUP BY plan_tier, ui_theme, event_type
ORDER BY events DESC
LIMIT 50;
