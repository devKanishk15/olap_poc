-- GQ02 — Filtered aggregate: only enabled listings (glusr_premium_enable = '1')
-- Tests predicate selectivity on a low-cardinality column over a full GCS scan.
-- Even with the WHERE filter, all rows are scanned (CSV has no skip index).
-- Dialect: Apache Doris (s3() TVF)

SELECT
    COUNT(*)                               AS total_enabled,
    COUNT(DISTINCT fk_glusr_usr_id)        AS distinct_users,
    COUNT(DISTINCT glusr_premium_mcat_id)  AS distinct_mcats,
    MIN(glusr_premium_added_date)          AS earliest_enabled,
    MAX(last_modified_date)                AS latest_modified
FROM s3(
    "uri"              = "s3://<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>",
    "ENDPOINT"         = "https://storage.googleapis.com",
    "ACCESS_KEY"       = "<GCS_HMAC_ACCESS_KEY>",
    "SECRET_KEY"       = "<GCS_HMAC_SECRET>",
    "format"           = "csv",
    "column_separator" = ",",
    "columns"          = "glusr_premium_listing_id BIGINT, fk_glusr_usr_id BIGINT, glusr_premium_mcat_id BIGINT, glusr_premium_city_id BIGINT, flag_premium_listing VARCHAR(10), fk_service_id BIGINT, fk_cust_to_serv_id BIGINT, pl_kwrd_term_upper VARCHAR(500), glusr_premium_enable VARCHAR(10), glusr_premium_added_date DATETIME, last_modified_date DATETIME, glusr_premium_updatedby_id BIGINT, glusr_premium_updatedby VARCHAR(255), glusr_premium_updatescreen VARCHAR(255), glusr_premium_ip VARCHAR(100), glusr_premium_ip_country VARCHAR(40), glusr_premium_hist_comments VARCHAR(1000), glusr_premium_updatedby_url VARCHAR(255), category_type VARCHAR(50), location_type VARCHAR(50), location_iso VARCHAR(10), category_location_credit_value DOUBLE"
)
WHERE glusr_premium_enable = '1'
