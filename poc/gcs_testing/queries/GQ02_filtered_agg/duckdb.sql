-- GQ02 — Filtered aggregate: only enabled listings (glusr_premium_enable = '1')
-- Tests predicate selectivity on a low-cardinality column over a full GCS scan.
-- Even with the WHERE filter, all rows are scanned (CSV has no skip index).
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    COUNT(*)                               AS total_enabled,
    COUNT(DISTINCT fk_glusr_usr_id)        AS distinct_users,
    COUNT(DISTINCT glusr_premium_mcat_id)  AS distinct_mcats,
    MIN(glusr_premium_added_date)          AS earliest_enabled,
    MAX(last_modified_date)                AS latest_modified
FROM read_csv_auto(
    's3://<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    header = true,
    null_padding = true
)
WHERE glusr_premium_enable = '1'
