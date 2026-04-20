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
    "s3.endpoint"      = "storage.asia-south1.rep.googleapis.com",
    "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
    "s3.secret_key"    = "<GCS_HMAC_SECRET>",
    "s3.region"        = "<GCS_REGION>",
    "provider"         = "GCP",
    "format"           = "csv",
    "column_separator" = ",",
    "csv_schema"       = "glusr_premium_listing_id:bigint;fk_glusr_usr_id:bigint;glusr_premium_mcat_id:bigint;glusr_premium_city_id:bigint;flag_premium_listing:string;fk_service_id:bigint;fk_cust_to_serv_id:bigint;pl_kwrd_term_upper:string;glusr_premium_enable:string;glusr_premium_added_date:datetime;last_modified_date:datetime;glusr_premium_updatedby_id:bigint;glusr_premium_updatedby:string;glusr_premium_updatescreen:string;glusr_premium_ip:string;glusr_premium_ip_country:string;glusr_premium_hist_comments:string;glusr_premium_updatedby_url:string;category_type:string;location_type:string;location_iso:string;category_location_credit_value:double"
)
WHERE glusr_premium_enable = '1'
