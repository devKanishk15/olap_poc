-- GQ01 — Full scan + aggregate over glusr_premium_listing CSV in GCS
-- Tests raw GCS-to-engine I/O throughput.
-- No filter; forces a complete file scan.
-- Dialect: ClickHouse (s3() table function, CSVWithNames)
-- Dialect differences vs Doris/DuckDB:
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   SUM(CASE WHEN ...) → countIf(...)
--   Uses CSVWithNames (header row provides column names; all types inferred as String)
--   No trailing semicolon — runner appends FORMAT JSON

SELECT
    count()                                                   AS total_listings,
    uniqExact(fk_glusr_usr_id)                                AS distinct_users,
    uniqExact(glusr_premium_mcat_id)                          AS distinct_mcats,
    min(glusr_premium_added_date)                             AS earliest_listing,
    max(glusr_premium_added_date)                             AS latest_listing,
    countIf(glusr_premium_enable = '1')                       AS enabled_count
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>',
    '<GCS_HMAC_SECRET>',
    'CSVWithNames'
)
