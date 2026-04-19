-- GQ05 — Date range filter on glusr_premium_added_date (one quarter: 2024-Q1)
-- Measures how each engine handles a time-bounded scan over an unpartitioned CSV.
-- All rows are scanned (no skip index in CSV); filter is applied post-read.
-- Adjust the date range to match actual data if needed.
-- Dialect: Apache Doris (s3() TVF)

SELECT
    CAST(glusr_premium_added_date AS DATE) AS added_day,
    category_type,
    COUNT(*)                               AS listings_added,
    COUNT(DISTINCT fk_glusr_usr_id)        AS active_users
FROM s3(
    "uri"              = "s3://<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>",
    "s3.endpoint"      = "https://storage.googleapis.com",
    "s3.region"        = "<GCS_REGION>",
    "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
    "s3.secret_key"    = "<GCS_HMAC_SECRET>",
    "format"           = "csv",
    "column_separator" = ",",
    "columns"          = "glusr_premium_listing_id BIGINT, fk_glusr_usr_id BIGINT, glusr_premium_mcat_id BIGINT, glusr_premium_city_id BIGINT, flag_premium_listing VARCHAR(10), fk_service_id BIGINT, fk_cust_to_serv_id BIGINT, pl_kwrd_term_upper VARCHAR(500), glusr_premium_enable VARCHAR(10), glusr_premium_added_date DATETIME, last_modified_date DATETIME, glusr_premium_updatedby_id BIGINT, glusr_premium_updatedby VARCHAR(255), glusr_premium_updatescreen VARCHAR(255), glusr_premium_ip VARCHAR(100), glusr_premium_ip_country VARCHAR(40), glusr_premium_hist_comments VARCHAR(1000), glusr_premium_updatedby_url VARCHAR(255), category_type VARCHAR(50), location_type VARCHAR(50), location_iso VARCHAR(10), category_location_credit_value DOUBLE"
)
WHERE glusr_premium_added_date BETWEEN '2024-01-01 00:00:00' AND '2024-03-31 23:59:59'
GROUP BY 1, 2
ORDER BY 1 ASC, listings_added DESC
