-- GQ10 — Heavy multi-column scan (deliberate memory pressure / spill candidate)
-- Reads all wide text columns: pl_kwrd_term_upper (500), glusr_premium_hist_comments (1000),
-- glusr_premium_updatedby_url (255), glusr_premium_updatedby (255).
-- Two-level CTE with 2-column GROUP BY; designed to exhaust 8 GB RAM and trigger spill.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials + SET memory_limit = '6GB'
--       + SET temp_directory before executing this file. Spill is expected.

WITH per_user_category AS (
    SELECT
        fk_glusr_usr_id,
        category_type,
        COUNT(*)                                                                      AS listing_count,
        COUNT(DISTINCT glusr_premium_mcat_id)                                         AS mcat_count,
        SUM(CASE WHEN glusr_premium_hist_comments IS NOT NULL
                  AND glusr_premium_hist_comments <> '' THEN 1 ELSE 0 END)            AS has_comment_count,
        SUM(CASE WHEN pl_kwrd_term_upper LIKE '%PREMIUM%' THEN 1 ELSE 0 END)          AS premium_keyword_count,
        SUM(CASE WHEN glusr_premium_updatedby_url LIKE '%http%' THEN 1 ELSE 0 END)   AS has_url_count,
        MAX(last_modified_date)                                                       AS last_update
    FROM read_csv_auto(
        's3://<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
        header = true,
        null_padding = true
    )
    GROUP BY fk_glusr_usr_id, category_type
)
SELECT
    category_type,
    COUNT(DISTINCT fk_glusr_usr_id)   AS users,
    SUM(listing_count)                AS total_listings,
    SUM(mcat_count)                   AS total_mcats,
    SUM(has_comment_count)            AS listings_with_comments,
    SUM(premium_keyword_count)        AS premium_keyword_total,
    SUM(has_url_count)                AS url_total,
    AVG(listing_count)                AS avg_listings_per_user_category
FROM per_user_category
GROUP BY category_type
ORDER BY total_listings DESC
LIMIT 200
