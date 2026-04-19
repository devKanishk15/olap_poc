-- GQ03 — GROUP BY low-cardinality column (category_type, ~5-10 distinct values)
-- Hash aggregation fits entirely in cache; isolates parsing/network overhead from compute.
-- Dialect: ClickHouse (s3() table function, CSV + schema string)
-- Dialect differences vs Doris/DuckDB:
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   No trailing semicolon — runner appends FORMAT JSON

SELECT
    category_type,
    count()                                AS total_listings,
    uniqExact(fk_glusr_usr_id)             AS distinct_users,
    uniqExact(glusr_premium_mcat_id)       AS distinct_mcats,
    min(glusr_premium_added_date)          AS earliest_listing,
    max(last_modified_date)                AS latest_modified
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>',
    '<GCS_HMAC_SECRET>',
    'CSV',
    'glusr_premium_listing_id UInt64, fk_glusr_usr_id UInt64, glusr_premium_mcat_id UInt64, glusr_premium_city_id UInt64, flag_premium_listing String, fk_service_id UInt64, fk_cust_to_serv_id UInt64, pl_kwrd_term_upper String, glusr_premium_enable String, glusr_premium_added_date DateTime, last_modified_date DateTime, glusr_premium_updatedby_id UInt64, glusr_premium_updatedby String, glusr_premium_updatescreen String, glusr_premium_ip String, glusr_premium_ip_country String, glusr_premium_hist_comments String, glusr_premium_updatedby_url String, category_type String, location_type String, location_iso String, category_location_credit_value Float64'
)
GROUP BY category_type
ORDER BY total_listings DESC
