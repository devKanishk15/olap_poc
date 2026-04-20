-- GQ08 — Approximate vs exact distinct count comparison
-- Compares APPROX_COUNT_DISTINCT (HyperLogLog) vs COUNT(DISTINCT) per category bucket.
-- Highlights engines that have native HLL support vs those that materialise the full set.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    category_type,
    COUNT(*)                                         AS total_listings,
    APPROX_COUNT_DISTINCT(fk_glusr_usr_id)           AS approx_distinct_users,
    COUNT(DISTINCT fk_glusr_usr_id)                  AS exact_distinct_users,
    APPROX_COUNT_DISTINCT(glusr_premium_mcat_id)     AS approx_distinct_mcats
FROM read_csv_auto(
    's3://<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    header = true,
    null_padding = true
)
GROUP BY category_type
ORDER BY total_listings DESC
