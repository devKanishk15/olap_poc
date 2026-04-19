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
        "uri"              = "https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>",
        "s3.region"        = "<GCS_REGION>",
        "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
        "s3.secret_key"    = "<GCS_HMAC_SECRET>",
        "use_path_style"   = "true",
        "format"           = "csv",
        "column_separator" = ",",
        "columns"          = "glusr_premium_listing_id BIGINT, fk_glusr_usr_id BIGINT, glusr_premium_mcat_id BIGINT, glusr_premium_city_id BIGINT, flag_premium_listing VARCHAR(10), fk_service_id BIGINT, fk_cust_to_serv_id BIGINT, pl_kwrd_term_upper VARCHAR(500), glusr_premium_enable VARCHAR(10), glusr_premium_added_date DATETIME, last_modified_date DATETIME, glusr_premium_updatedby_id BIGINT, glusr_premium_updatedby VARCHAR(255), glusr_premium_updatescreen VARCHAR(255), glusr_premium_ip VARCHAR(100), glusr_premium_ip_country VARCHAR(40), glusr_premium_hist_comments VARCHAR(1000), glusr_premium_updatedby_url VARCHAR(255), category_type VARCHAR(50), location_type VARCHAR(50), location_iso VARCHAR(10), category_location_credit_value DOUBLE"
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
