-- Q11 — JSON field extraction
-- Tests: semi-structured data handling; custom_dimensions is a JSON string column
-- Extracts specific keys from the JSON payload and aggregates
-- Dialect: Apache Doris
-- NOTE: Doris supports JSON functions since 2.0. custom_dimensions stored as JSON type.

SELECT
    JSON_EXTRACT_STRING(custom_dimensions, '$.plan')    AS plan_tier,
    JSON_EXTRACT_STRING(custom_dimensions, '$.theme')   AS ui_theme,
    event_type,
    COUNT(*)                                             AS events,
    SUM(revenue)                                         AS total_revenue,
    AVG(duration_ms)                                     AS avg_duration_ms
FROM poc.event_fact
WHERE
    custom_dimensions IS NOT NULL
    AND event_date BETWEEN '2024-01-01' AND '2024-01-30'
GROUP BY plan_tier, ui_theme, event_type
ORDER BY events DESC
LIMIT 50;
