-- GQ07 — String LIKE scan on keyword and comment columns
-- Stresses string scanning on wide columns: pl_kwrd_term_upper (varchar 500),
-- glusr_premium_hist_comments (varchar 1000), glusr_premium_updatedby_url (varchar 255).
-- High I/O cost per row due to column widths.
-- Dialect: ClickHouse (s3() table function, CSV + schema string)
-- Dialect differences vs Doris/DuckDB:
--   Standard LIKE works in ClickHouse; like(col, pattern) function form also valid
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   No trailing semicolon — runner appends FORMAT JSON

SELECT
    category_type,
    count()                                AS matched_listings,
    uniqExact(fk_glusr_usr_id)             AS users_with_match
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>',
    '<GCS_HMAC_SECRET>',
    'CSV',
    'glusr_premium_listing_id Int64, fk_glusr_usr_id Int64, glusr_premium_mcat_id Int64, glusr_premium_city_id Int64, flag_premium_listing String, fk_service_id Int64, fk_cust_to_serv_id Int64, pl_kwrd_term_upper String, glusr_premium_enable String, glusr_premium_added_date DateTime64(6), last_modified_date DateTime64(6), glusr_premium_updatedby_id Int64, glusr_premium_updatedby String, glusr_premium_updatescreen String, glusr_premium_ip String, glusr_premium_ip_country String, glusr_premium_hist_comments String, glusr_premium_updatedby_url String, category_type String, location_type String, location_iso String, category_location_credit_value Float64'
)
WHERE
    like(pl_kwrd_term_upper, '%PREMIUM%')
    OR like(glusr_premium_hist_comments, '%approved%')
    OR like(glusr_premium_updatedby_url, '%http%')
GROUP BY category_type
ORDER BY matched_listings DESC
SETTINGS input_format_csv_empty_as_default = 1
