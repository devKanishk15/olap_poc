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
    "s3.endpoint"      = "storage.asia-south1.rep.googleapis.com",
    "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
    "s3.secret_key"    = "<GCS_HMAC_SECRET>",
    "s3.region"        = "<GCS_REGION>",
    "provider"         = "GCP",
    "format"           = "csv",
    "column_separator" = ",",
    "csv_schema"       = "glusr_premium_listing_id:bigint;fk_glusr_usr_id:bigint;glusr_premium_mcat_id:bigint;glusr_premium_city_id:bigint;flag_premium_listing:string;fk_service_id:bigint;fk_cust_to_serv_id:bigint;pl_kwrd_term_upper:string;glusr_premium_enable:string;glusr_premium_added_date:datetime;last_modified_date:datetime;glusr_premium_updatedby_id:bigint;glusr_premium_updatedby:string;glusr_premium_updatescreen:string;glusr_premium_ip:string;glusr_premium_ip_country:string;glusr_premium_hist_comments:string;glusr_premium_updatedby_url:string;category_type:string;location_type:string;location_iso:string;category_location_credit_value:double"
)
