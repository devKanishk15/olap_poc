-- GQ10 — Heavy multi-column scan (deliberate memory pressure / spill candidate)
-- Reads all wide text columns: pl_kwrd_term_upper (500), glusr_premium_hist_comments (1000),
-- glusr_premium_updatedby_url (255), glusr_premium_updatedby (255).
-- Two-level CTE with 3-column GROUP BY; designed to exhaust 8 GB RAM and trigger spill.
-- Dialect: ClickHouse (s3() table function, CSV + schema string)
-- Dialect differences vs Doris/DuckDB:
--   SUM(CASE WHEN col IS NOT NULL ...) → countIf(isNotNull(col))
--   SUM(CASE WHEN col LIKE ...) → countIf(like(col, pattern))
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   AVG(x) → avg(x)
--   SETTINGS clause enables spill-to-disk rather than OOM for inner GROUP BY
--   No trailing semicolon — runner appends FORMAT JSON

WITH per_user_category_country AS (
    SELECT
        fk_glusr_usr_id,
        category_type,
        glusr_premium_ip_country,
        count()                                                              AS listing_count,
        uniqExact(glusr_premium_mcat_id)                                     AS mcat_count,
        countIf(isNotNull(glusr_premium_hist_comments))                      AS has_comment_count,
        countIf(like(pl_kwrd_term_upper, '%PREMIUM%'))                       AS premium_keyword_count,
        countIf(like(glusr_premium_updatedby_url, '%http%'))                 AS has_url_count,
        max(last_modified_date)                                              AS last_update
    FROM s3(
        'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
        '<GCS_HMAC_ACCESS_KEY>',
        '<GCS_HMAC_SECRET>',
        'CSV',
        'glusr_premium_listing_id UInt64, fk_glusr_usr_id UInt64, glusr_premium_mcat_id UInt64, glusr_premium_city_id UInt64, flag_premium_listing String, fk_service_id UInt64, fk_cust_to_serv_id UInt64, pl_kwrd_term_upper String, glusr_premium_enable String, glusr_premium_added_date DateTime64(6), last_modified_date DateTime64(6), glusr_premium_updatedby_id UInt64, glusr_premium_updatedby String, glusr_premium_updatescreen String, glusr_premium_ip String, glusr_premium_ip_country String, glusr_premium_hist_comments String, glusr_premium_updatedby_url String, category_type String, location_type String, location_iso String, category_location_credit_value Float64'
    )
    GROUP BY fk_glusr_usr_id, category_type, glusr_premium_ip_country
)
SELECT
    category_type,
    glusr_premium_ip_country,
    uniqExact(fk_glusr_usr_id)        AS users,
    sum(listing_count)                AS total_listings,
    sum(mcat_count)                   AS total_mcats,
    sum(has_comment_count)            AS listings_with_comments,
    sum(premium_keyword_count)        AS premium_keyword_total,
    sum(has_url_count)                AS url_total,
    avg(listing_count)                AS avg_listings_per_user_category
FROM per_user_category_country
GROUP BY category_type, glusr_premium_ip_country
ORDER BY total_listings DESC
LIMIT 200
SETTINGS max_bytes_before_external_group_by = 3000000000
