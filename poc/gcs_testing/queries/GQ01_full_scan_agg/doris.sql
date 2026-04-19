-- GQ01 — Full scan + aggregate over glusr_premium_listing CSV in GCS
-- Tests raw GCS-to-engine I/O throughput.
-- No filter; forces a complete file scan.
-- Dialect: Apache Doris (s3() TVF)

SELECT
    COUNT(*)                                                              AS total_listings,
    COUNT(DISTINCT fk_glusr_usr_id)                                       AS distinct_users,
    COUNT(DISTINCT glusr_premium_mcat_id)                                 AS distinct_mcats,
    MIN(glusr_premium_added_date)                                         AS earliest_listing,
    MAX(glusr_premium_added_date)                                         AS latest_listing,
    SUM(CASE WHEN glusr_premium_enable = '1' THEN 1 ELSE 0 END)          AS enabled_count
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
