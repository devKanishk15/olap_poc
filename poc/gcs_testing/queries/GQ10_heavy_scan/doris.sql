-- GQ10 — Heavy multi-column scan (deliberate memory pressure / spill candidate)
-- Reads all wide text columns: pl_kwrd_term_upper (500), glusr_premium_hist_comments (1000),
-- glusr_premium_updatedby_url (255), glusr_premium_updatedby (255).
-- Two-level CTE with 3-column GROUP BY; designed to exhaust 8 GB RAM and trigger spill.
-- Dialect: Apache Doris (s3() TVF)

WITH per_user_category_country AS (
    SELECT
        fk_glusr_usr_id,
        category_type,
        glusr_premium_ip_country,
        COUNT(*)                                                                     AS listing_count,
        COUNT(DISTINCT glusr_premium_mcat_id)                                        AS mcat_count,
        SUM(CASE WHEN glusr_premium_hist_comments IS NOT NULL THEN 1 ELSE 0 END)     AS has_comment_count,
        SUM(CASE WHEN pl_kwrd_term_upper LIKE '%PREMIUM%' THEN 1 ELSE 0 END)         AS premium_keyword_count,
        SUM(CASE WHEN glusr_premium_updatedby_url LIKE '%http%' THEN 1 ELSE 0 END)  AS has_url_count,
        MAX(last_modified_date)                                                      AS last_update
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
    GROUP BY fk_glusr_usr_id, category_type, glusr_premium_ip_country
)
SELECT
    category_type,
    glusr_premium_ip_country,
    COUNT(DISTINCT fk_glusr_usr_id)   AS users,
    SUM(listing_count)                AS total_listings,
    SUM(mcat_count)                   AS total_mcats,
    SUM(has_comment_count)            AS listings_with_comments,
    SUM(premium_keyword_count)        AS premium_keyword_total,
    SUM(has_url_count)                AS url_total,
    AVG(listing_count)                AS avg_listings_per_user_category
FROM per_user_category_country
GROUP BY category_type, glusr_premium_ip_country
ORDER BY total_listings DESC
LIMIT 200
