-- Q11 — JSON field extraction
-- Dialect differences:
--   DuckDB uses json_extract_string() or the ->> operator (SQL/JSON pointer)
--   custom_dimensions stored as JSON type in DuckDB; arrow operator works natively
-- Dialect: DuckDB

SELECT
    custom_dimensions ->> '$.plan'       AS plan_tier,
    custom_dimensions ->> '$.theme'      AS ui_theme,
    event_type,
    COUNT(*)                             AS events,
    SUM(revenue)                         AS total_revenue,
    AVG(duration_ms)                     AS avg_duration_ms
FROM poc.event_fact
WHERE
    custom_dimensions IS NOT NULL
    AND event_date BETWEEN '2024-01-01' AND '2024-01-30'
GROUP BY plan_tier, ui_theme, event_type
ORDER BY events DESC
LIMIT 50;
