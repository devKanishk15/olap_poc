-- GQ04 — GROUP BY high-cardinality column (fk_glusr_usr_id, user IDs)
-- Expected to have thousands of distinct users; likely to stress memory on 8 GB VM.
-- Dialect: Apache Doris (s3() TVF)

SELECT
    fk_glusr_usr_id,
    COUNT(*)                                                              AS listing_count,
    COUNT(DISTINCT glusr_premium_mcat_id)                                 AS distinct_mcats,
    SUM(CASE WHEN glusr_premium_enable = '1' THEN 1 ELSE 0 END)          AS enabled_count,
    SUM(CASE WHEN flag_premium_listing = '1' THEN 1 ELSE 0 END)          AS premium_count,
    MAX(last_modified_date)                                               AS last_activity
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
GROUP BY fk_glusr_usr_id
ORDER BY listing_count DESC
LIMIT 1000
