-- GQ10 — Heavy multi-column scan (spill candidate)
-- Dialect: Trino 481
-- Trino spills automatically to /data/trino/spill under memory pressure.

WITH per_user_category_country AS (
    SELECT
        fk_glusr_usr_id,
        category_type,
        glusr_premium_ip_country,
        COUNT(*)                                                                    AS listing_count,
        COUNT(DISTINCT glusr_premium_mcat_id)                                       AS mcat_count,
        SUM(CASE WHEN glusr_premium_hist_comments IS NOT NULL THEN 1 ELSE 0 END)    AS has_comment_count,
        SUM(CASE WHEN pl_kwrd_term_upper LIKE '%PREMIUM%' THEN 1 ELSE 0 END)        AS premium_keyword_count,
        SUM(CASE WHEN glusr_premium_updatedby_url LIKE '%http%' THEN 1 ELSE 0 END)  AS has_url_count,
        MAX(last_modified_date)                                                     AS last_update
    FROM poc.glusr_premium_listing
    GROUP BY fk_glusr_usr_id, category_type, glusr_premium_ip_country
)
SELECT
    category_type,
    glusr_premium_ip_country,
    COUNT(DISTINCT fk_glusr_usr_id)     AS users,
    SUM(listing_count)                  AS total_listings,
    SUM(mcat_count)                     AS total_mcats,
    SUM(has_comment_count)              AS listings_with_comments,
    SUM(premium_keyword_count)          AS premium_keyword_total,
    SUM(has_url_count)                  AS url_total,
    AVG(CAST(listing_count AS DOUBLE))  AS avg_listings_per_user_category
FROM per_user_category_country
GROUP BY category_type, glusr_premium_ip_country
ORDER BY total_listings DESC
LIMIT 200
