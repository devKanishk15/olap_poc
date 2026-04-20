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
GROUP BY fk_glusr_usr_id
ORDER BY listing_count DESC
LIMIT 1000
