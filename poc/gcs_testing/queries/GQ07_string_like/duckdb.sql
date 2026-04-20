-- GQ07 — String LIKE scan on keyword column
-- Stresses string scanning on wide column: pl_kwrd_term_upper (varchar 500).
-- glusr_premium_hist_comments and glusr_premium_updatedby_url absent from this CSV export.
-- High I/O cost per row due to column width.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    category_type,
    COUNT(*)                               AS matched_listings,
    COUNT(DISTINCT fk_glusr_usr_id)        AS users_with_match
FROM read_csv_auto(
    's3://<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    header = true,
    null_padding = true
)
WHERE
    pl_kwrd_term_upper LIKE '%PREMIUM%'
GROUP BY category_type
ORDER BY matched_listings DESC
