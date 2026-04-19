-- GQ04 — GROUP BY high-cardinality column (fk_glusr_usr_id, user IDs)
-- Expected to have thousands of distinct users; likely to stress memory on 8 GB VM.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    fk_glusr_usr_id,
    COUNT(*)                                                              AS listing_count,
    COUNT(DISTINCT glusr_premium_mcat_id)                                 AS distinct_mcats,
    SUM(CASE WHEN glusr_premium_enable = '1' THEN 1 ELSE 0 END)          AS enabled_count,
    SUM(CASE WHEN flag_premium_listing = '1' THEN 1 ELSE 0 END)          AS premium_count,
    MAX(last_modified_date)                                               AS last_activity
FROM read_csv_auto(
    's3://<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    header = true,
    null_padding = true
)
GROUP BY fk_glusr_usr_id
ORDER BY listing_count DESC
LIMIT 1000
