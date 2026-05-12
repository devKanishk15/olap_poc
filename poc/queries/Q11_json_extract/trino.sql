-- Q11 — JSON field extraction
-- Dialect: Trino 481
-- Trino: json_extract_scalar(col, '$.key') — standard JSONPath.
-- Dialect difference: JSON_EXTRACT_STRING(x, '$.k') in Doris → json_extract_scalar(x, '$.k') in Trino.

SELECT
    json_extract_scalar(custom_dimensions, '$.plan')     AS plan_tier,
    json_extract_scalar(custom_dimensions, '$.theme')    AS ui_theme,
    event_type,
    COUNT(*)                                              AS events,
    SUM(revenue)                                          AS total_revenue,
    AVG(CAST(duration_ms AS DOUBLE))                      AS avg_duration_ms
FROM poc.event_fact
WHERE
    custom_dimensions IS NOT NULL
    AND custom_dimensions <> ''
    AND event_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-30'
GROUP BY
    json_extract_scalar(custom_dimensions, '$.plan'),
    json_extract_scalar(custom_dimensions, '$.theme'),
    event_type
ORDER BY events DESC
LIMIT 50
