-- GQ08 — Approximate vs exact distinct count comparison
-- Compares APPROX_COUNT_DISTINCT (HyperLogLog) vs COUNT(DISTINCT) per category bucket.
-- Highlights engines that have native HLL support vs those that materialise the full set.
-- Dialect: Apache Doris (s3() TVF)

SELECT
    category_type,
    COUNT(*)                                         AS total_listings,
    APPROX_COUNT_DISTINCT(fk_glusr_usr_id)           AS approx_distinct_users,
    COUNT(DISTINCT fk_glusr_usr_id)                  AS exact_distinct_users,
    APPROX_COUNT_DISTINCT(glusr_premium_mcat_id)     AS approx_distinct_mcats
FROM s3(
    "uri"              = "s3://<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>",
    "s3.endpoint"      = "https://storage.googleapis.com",
    "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
    "s3.secret_key"    = "<GCS_HMAC_SECRET>",
    "format"           = "csv",
    "column_separator" = ",",
    "columns"          = "glusr_premium_listing_id BIGINT, fk_glusr_usr_id BIGINT, glusr_premium_mcat_id BIGINT, glusr_premium_city_id BIGINT, flag_premium_listing VARCHAR(10), fk_service_id BIGINT, fk_cust_to_serv_id BIGINT, pl_kwrd_term_upper VARCHAR(500), glusr_premium_enable VARCHAR(10), glusr_premium_added_date DATETIME, last_modified_date DATETIME, glusr_premium_updatedby_id BIGINT, glusr_premium_updatedby VARCHAR(255), glusr_premium_updatescreen VARCHAR(255), glusr_premium_ip VARCHAR(100), glusr_premium_ip_country VARCHAR(40), glusr_premium_hist_comments VARCHAR(1000), glusr_premium_updatedby_url VARCHAR(255), category_type VARCHAR(50), location_type VARCHAR(50), location_iso VARCHAR(10), category_location_credit_value DOUBLE"
)
GROUP BY category_type
ORDER BY total_listings DESC
