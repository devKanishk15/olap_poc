-- GQ01 — Full scan + aggregate over glusr_premium_listing CSV in GCS
-- Tests raw GCS-to-engine I/O throughput.
-- No filter; forces a complete file scan.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    COUNT(*)                                                              AS total_listings,
    COUNT(DISTINCT fk_glusr_usr_id)                                       AS distinct_users,
    COUNT(DISTINCT glusr_premium_mcat_id)                                 AS distinct_mcats,
    MIN(glusr_premium_added_date)                                         AS earliest_listing,
    MAX(glusr_premium_added_date)                                         AS latest_listing,
    SUM(CASE WHEN glusr_premium_enable = '1' THEN 1 ELSE 0 END)          AS enabled_count
FROM read_csv_auto(
    's3://<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    header = true,
    null_padding = true
)
