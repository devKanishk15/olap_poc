-- GQ07 — String LIKE scan on keyword and comment columns
-- Stresses string scanning on wide columns: pl_kwrd_term_upper (varchar 500),
-- glusr_premium_hist_comments (varchar 1000), glusr_premium_updatedby_url (varchar 255).
-- High I/O cost per row due to column widths.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    category_type,
    COUNT(*)                               AS matched_listings,
    COUNT(DISTINCT fk_glusr_usr_id)        AS users_with_match
FROM read_csv_auto(
    's3://<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    header = true,
    null_padding = true
)
WHERE
    pl_kwrd_term_upper              LIKE '%PREMIUM%'
    OR glusr_premium_hist_comments  LIKE '%approved%'
    OR glusr_premium_updatedby_url  LIKE '%http%'
GROUP BY category_type
ORDER BY matched_listings DESC
