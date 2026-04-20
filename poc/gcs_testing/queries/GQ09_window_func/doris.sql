-- GQ09 — Window function: rank users within each category_type by listing count
-- Two-level CTE: first aggregates per (fk_glusr_usr_id, category_type), then applies window functions.
-- Memory-intensive; requires buffering the intermediate aggregate result.
-- Dialect: Apache Doris (s3() TVF)

WITH user_category_agg AS (
    SELECT
        fk_glusr_usr_id,
        category_type,
        COUNT(*)                          AS listing_count,
        COUNT(DISTINCT glusr_premium_mcat_id) AS mcat_count,
        MAX(last_modified_date)           AS last_update
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
    GROUP BY fk_glusr_usr_id, category_type
)
SELECT
    fk_glusr_usr_id,
    category_type,
    listing_count,
    mcat_count,
    ROW_NUMBER() OVER (
        PARTITION BY category_type
        ORDER BY listing_count DESC
    )                                     AS rank_within_category,
    SUM(listing_count) OVER (
        PARTITION BY category_type
        ORDER BY listing_count DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                     AS running_listing_total
FROM user_category_agg
ORDER BY category_type, rank_within_category
LIMIT 500
