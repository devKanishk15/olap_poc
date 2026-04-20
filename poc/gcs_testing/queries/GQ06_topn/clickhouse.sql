-- GQ06 — TOP-N users by listing count (LIMIT 100)
-- Tests partial-sort / top-heap optimisation; engines should not sort the entire
-- aggregated result before applying LIMIT.
-- Dialect: ClickHouse (s3() table function, CSV + schema string)
-- Dialect differences vs Doris/DuckDB:
--   IS NOT NULL → isNotNull()
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   SUM(CASE WHEN ...) → countIf(...)
--   No trailing semicolon — runner appends FORMAT JSON

SELECT
    fk_glusr_usr_id,
    count()                                AS total_listings,
    uniqExact(glusr_premium_mcat_id)       AS distinct_mcats,
    countIf(glusr_premium_enable = '1')    AS enabled_listings,
    max(last_modified_date)                AS last_update
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>',
    '<GCS_HMAC_SECRET>',
    'CSV',
    'glusr_premium_listing_id Int64, fk_glusr_usr_id Int64, glusr_premium_mcat_id Int64, glusr_premium_city_id Int64, flag_premium_listing String, fk_service_id Int64, fk_cust_to_serv_id Int64, pl_kwrd_term_upper String, glusr_premium_enable String, glusr_premium_added_date DateTime64(6), last_modified_date DateTime64(6), glusr_premium_updatedby_id Int64, glusr_premium_updatedby String, glusr_premium_updatescreen String, glusr_premium_ip String, glusr_premium_ip_country String, glusr_premium_hist_comments String, glusr_premium_updatedby_url String, category_type String, location_type String, location_iso String, category_location_credit_value Float64'
)
WHERE isNotNull(fk_glusr_usr_id)
GROUP BY fk_glusr_usr_id
ORDER BY total_listings DESC
LIMIT 100
SETTINGS input_format_csv_empty_as_default = 1
