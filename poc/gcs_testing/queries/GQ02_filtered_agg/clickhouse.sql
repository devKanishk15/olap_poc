-- GQ02 — Filtered aggregate: only enabled listings (glusr_premium_enable = '1')
-- Tests predicate selectivity on a low-cardinality column over a full GCS scan.
-- Even with the WHERE filter, all rows are scanned (CSV has no skip index).
-- Dialect: ClickHouse (s3() table function, CSV + schema string)
-- Dialect differences vs Doris/DuckDB:
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   No trailing semicolon — runner appends FORMAT JSON

SELECT
    count()                                AS total_enabled,
    uniqExact(fk_glusr_usr_id)             AS distinct_users,
    uniqExact(glusr_premium_mcat_id)       AS distinct_mcats,
    min(glusr_premium_added_date)          AS earliest_enabled,
    max(last_modified_date)                AS latest_modified
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>',
    '<GCS_HMAC_SECRET>',
    'CSV',
    'glusr_premium_listing_id UInt64, fk_glusr_usr_id UInt64, glusr_premium_mcat_id UInt64, glusr_premium_city_id UInt64, flag_premium_listing String, fk_service_id UInt64, fk_cust_to_serv_id UInt64, pl_kwrd_term_upper String, glusr_premium_enable String, glusr_premium_added_date DateTime64(6), last_modified_date DateTime64(6), glusr_premium_updatedby_id UInt64, glusr_premium_updatedby String, glusr_premium_updatescreen String, glusr_premium_ip String, glusr_premium_ip_country String, glusr_premium_hist_comments String, glusr_premium_updatedby_url String, category_type String, location_type String, location_iso String, category_location_credit_value Float64'
)
WHERE glusr_premium_enable = '1'
