-- GQ06 — TOP-N users by listing count (LIMIT 100)
-- Tests partial-sort / top-heap optimisation; engines should not sort the entire
-- aggregated result before applying LIMIT.
-- Dialect: Apache Doris (s3() TVF)

SELECT
    fk_glusr_usr_id,
    COUNT(*)                               AS total_listings,
    COUNT(DISTINCT glusr_premium_mcat_id)  AS distinct_mcats,
    SUM(CASE WHEN glusr_premium_enable = '1' THEN 1 ELSE 0 END) AS enabled_listings,
    MAX(last_modified_date)                AS last_update
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
WHERE fk_glusr_usr_id IS NOT NULL
GROUP BY fk_glusr_usr_id
ORDER BY total_listings DESC
LIMIT 100
