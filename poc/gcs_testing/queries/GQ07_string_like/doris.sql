-- GQ07 — String LIKE scan on keyword and comment columns
-- Stresses string scanning on wide columns: pl_kwrd_term_upper (varchar 500),
-- glusr_premium_hist_comments (varchar 1000), glusr_premium_updatedby_url (varchar 255).
-- High I/O cost per row due to column widths.
-- Dialect: Apache Doris (s3() TVF)

SELECT
    category_type,
    COUNT(*)                               AS matched_listings,
    COUNT(DISTINCT fk_glusr_usr_id)        AS users_with_match
FROM s3(
    "uri"              = "https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>",
    "s3.region"        = "<GCS_REGION>",
    "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
    "s3.secret_key"    = "<GCS_HMAC_SECRET>",
    "use_path_style"   = "true",
    "format"           = "csv",
    "column_separator" = ",",
    "columns"          = "glusr_premium_listing_id BIGINT, fk_glusr_usr_id BIGINT, glusr_premium_mcat_id BIGINT, glusr_premium_city_id BIGINT, flag_premium_listing VARCHAR(10), fk_service_id BIGINT, fk_cust_to_serv_id BIGINT, pl_kwrd_term_upper VARCHAR(500), glusr_premium_enable VARCHAR(10), glusr_premium_added_date DATETIME, last_modified_date DATETIME, glusr_premium_updatedby_id BIGINT, glusr_premium_updatedby VARCHAR(255), glusr_premium_updatescreen VARCHAR(255), glusr_premium_ip VARCHAR(100), glusr_premium_ip_country VARCHAR(40), glusr_premium_hist_comments VARCHAR(1000), glusr_premium_updatedby_url VARCHAR(255), category_type VARCHAR(50), location_type VARCHAR(50), location_iso VARCHAR(10), category_location_credit_value DOUBLE"
)
WHERE
    pl_kwrd_term_upper              LIKE '%PREMIUM%'
    OR glusr_premium_hist_comments  LIKE '%approved%'
    OR glusr_premium_updatedby_url  LIKE '%http%'
GROUP BY category_type
ORDER BY matched_listings DESC
