-- GQ03 — GROUP BY low-cardinality column (category_type, ~5-10 distinct values)
-- Hash aggregation fits entirely in cache; isolates parsing/network overhead from compute.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    category_type,
    COUNT(*)                               AS total_listings,
    COUNT(DISTINCT fk_glusr_usr_id)        AS distinct_users,
    COUNT(DISTINCT glusr_premium_mcat_id)  AS distinct_mcats,
    MIN(glusr_premium_added_date)          AS earliest_listing,
    MAX(last_modified_date)                AS latest_modified
FROM read_csv_auto(
    's3://<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    header = true,
    null_padding = true
)
GROUP BY category_type
ORDER BY total_listings DESC
