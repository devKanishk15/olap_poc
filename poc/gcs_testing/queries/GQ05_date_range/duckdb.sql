-- GQ05 — Date range filter on glusr_premium_added_date (one quarter: 2024-Q1)
-- Measures how each engine handles a time-bounded scan over an unpartitioned CSV.
-- All rows are scanned (no skip index in CSV); filter is applied post-read.
-- Adjust the date range to match actual data if needed.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    CAST(glusr_premium_added_date AS DATE) AS added_day,
    category_type,
    COUNT(*)                               AS listings_added,
    COUNT(DISTINCT fk_glusr_usr_id)        AS active_users
FROM read_csv_auto(
    's3://<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    header = true,
    null_padding = true
)
WHERE glusr_premium_added_date BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-03-31 23:59:59'
GROUP BY 1, 2
ORDER BY 1 ASC, listings_added DESC
