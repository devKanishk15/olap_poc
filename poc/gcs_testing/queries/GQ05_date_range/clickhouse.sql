-- GQ05 — Date range filter on glusr_premium_added_date (one quarter: 2024-Q1)
-- Measures how each engine handles a time-bounded scan over an unpartitioned CSV.
-- All rows are scanned (no skip index in CSV); filter is applied post-read.
-- Adjust the date range to match actual data if needed.
-- Dialect: ClickHouse (s3() table function, CSV + schema string)
-- Dialect differences vs Doris/DuckDB:
--   CAST(col AS DATE) → toDate(col)
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   No trailing semicolon — runner appends FORMAT JSON

SELECT
    toDate(glusr_premium_added_date)       AS added_day,
    category_type,
    count()                                AS listings_added,
    uniqExact(fk_glusr_usr_id)             AS active_users
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>',
    '<GCS_HMAC_SECRET>',
    'CSV',
    'glusr_premium_listing_id UInt64, fk_glusr_usr_id UInt64, glusr_premium_mcat_id UInt64, glusr_premium_city_id UInt64, flag_premium_listing String, fk_service_id UInt64, fk_cust_to_serv_id UInt64, pl_kwrd_term_upper String, glusr_premium_enable String, glusr_premium_added_date DateTime64(6), last_modified_date DateTime64(6), glusr_premium_updatedby_id UInt64, glusr_premium_updatedby String, glusr_premium_updatescreen String, glusr_premium_ip String, glusr_premium_ip_country String, glusr_premium_hist_comments String, glusr_premium_updatedby_url String, category_type String, location_type String, location_iso String, category_location_credit_value Float64'
)
WHERE glusr_premium_added_date BETWEEN toDateTime64('2024-01-01 00:00:00', 6) AND toDateTime64('2024-03-31 23:59:59.999999', 6)
GROUP BY added_day, category_type
ORDER BY added_day ASC, listings_added DESC
