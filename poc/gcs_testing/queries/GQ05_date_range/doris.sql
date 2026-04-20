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
    "s3.endpoint"      = "storage.asia-south1.rep.googleapis.com",
    "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
    "s3.secret_key"    = "<GCS_HMAC_SECRET>",
    "s3.region"        = "<GCS_REGION>",
    "provider"         = "GCP",
    "format"           = "csv",
    "column_separator" = ",",
    "csv_schema"       = "glusr_premium_listing_id:bigint;fk_glusr_usr_id:bigint;glusr_premium_mcat_id:bigint;glusr_premium_city_id:bigint;flag_premium_listing:string;fk_service_id:bigint;fk_cust_to_serv_id:bigint;pl_kwrd_term_upper:string;glusr_premium_enable:string;glusr_premium_added_date:datetime;last_modified_date:datetime;glusr_premium_updatedby_id:bigint;glusr_premium_updatedby:string;glusr_premium_updatescreen:string;glusr_premium_ip:string;glusr_premium_ip_country:string;glusr_premium_hist_comments:string;glusr_premium_updatedby_url:string;category_type:string;location_type:string;location_iso:string;category_location_credit_value:double"
)
WHERE glusr_premium_added_date BETWEEN '2024-01-01 00:00:00' AND '2024-03-31 23:59:59'
GROUP BY 1, 2
ORDER BY 1 ASC, listings_added DESC
