-- GQ09 — Window function: rank users within each category_type by listing count
-- Two-level CTE: first aggregates per (fk_glusr_usr_id, category_type), then applies window functions.
-- Memory-intensive; requires buffering the intermediate aggregate result.
-- Dialect: ClickHouse (s3() table function, CSV + schema string)
-- Dialect differences vs Doris/DuckDB:
--   IS NOT NULL → isNotNull()
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   Window function syntax is ANSI-compatible (ClickHouse 21.3+)
--   LIMIT 500 is important: ClickHouse materialises the full window frame before LIMIT
--   No trailing semicolon — runner appends FORMAT JSON

WITH user_category_agg AS (
    SELECT
        fk_glusr_usr_id,
        category_type,
        count()                        AS listing_count,
        uniqExact(glusr_premium_mcat_id) AS mcat_count,
        max(last_modified_date)        AS last_update
    FROM s3(
        'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
        '<GCS_HMAC_ACCESS_KEY>',
        '<GCS_HMAC_SECRET>',
        'CSV',
        'glusr_premium_listing_id UInt64, fk_glusr_usr_id UInt64, glusr_premium_mcat_id UInt64, glusr_premium_city_id UInt64, flag_premium_listing String, fk_service_id UInt64, fk_cust_to_serv_id UInt64, pl_kwrd_term_upper String, glusr_premium_enable String, glusr_premium_added_date DateTime, last_modified_date DateTime, glusr_premium_updatedby_id UInt64, glusr_premium_updatedby String, glusr_premium_updatescreen String, glusr_premium_ip String, glusr_premium_ip_country String, glusr_premium_hist_comments String, glusr_premium_updatedby_url String, category_type String, location_type String, location_iso String, category_location_credit_value Float64'
    )
    WHERE isNotNull(fk_glusr_usr_id)
    GROUP BY fk_glusr_usr_id, category_type
)
SELECT
    fk_glusr_usr_id,
    category_type,
    listing_count,
    mcat_count,
    row_number() OVER (
        PARTITION BY category_type
        ORDER BY listing_count DESC
    )                                     AS rank_within_category,
    sum(listing_count) OVER (
        PARTITION BY category_type
        ORDER BY listing_count DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                     AS running_listing_total
FROM user_category_agg
ORDER BY category_type, rank_within_category
LIMIT 500
