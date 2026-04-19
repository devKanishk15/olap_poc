-- GQ04 — GROUP BY high-cardinality column (fk_glusr_usr_id, user IDs)
-- Expected to have thousands of distinct users; likely to stress memory on 8 GB VM.
-- Dialect: ClickHouse (s3() table function, CSV + schema string)
-- Dialect differences vs Doris/DuckDB:
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   SUM(CASE WHEN ...) → countIf(...)
--   SETTINGS clause enables spill-to-disk for the GROUP BY rather than OOM
--   No trailing semicolon — runner appends FORMAT JSON

SELECT
    fk_glusr_usr_id,
    count()                                          AS listing_count,
    uniqExact(glusr_premium_mcat_id)                 AS distinct_mcats,
    countIf(glusr_premium_enable = '1')              AS enabled_count,
    countIf(flag_premium_listing = '1')              AS premium_count,
    max(last_modified_date)                          AS last_activity
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>',
    '<GCS_HMAC_SECRET>',
    'CSV',
    'glusr_premium_listing_id UInt64, fk_glusr_usr_id UInt64, glusr_premium_mcat_id UInt64, glusr_premium_city_id UInt64, flag_premium_listing String, fk_service_id UInt64, fk_cust_to_serv_id UInt64, pl_kwrd_term_upper String, glusr_premium_enable String, glusr_premium_added_date DateTime, last_modified_date DateTime, glusr_premium_updatedby_id UInt64, glusr_premium_updatedby String, glusr_premium_updatescreen String, glusr_premium_ip String, glusr_premium_ip_country String, glusr_premium_hist_comments String, glusr_premium_updatedby_url String, category_type String, location_type String, location_iso String, category_location_credit_value Float64'
)
GROUP BY fk_glusr_usr_id
ORDER BY listing_count DESC
LIMIT 1000
SETTINGS max_bytes_before_external_group_by = 3000000000
