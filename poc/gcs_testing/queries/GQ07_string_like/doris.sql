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
WHERE
    pl_kwrd_term_upper              LIKE '%PREMIUM%'
    OR glusr_premium_hist_comments  LIKE '%approved%'
    OR glusr_premium_updatedby_url  LIKE '%http%'
GROUP BY category_type
ORDER BY matched_listings DESC
