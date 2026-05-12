-- GQ09 — Window function: rank users within each category_type by listing count
-- Dialect: Trino 481
-- Full ANSI window functions — identical semantics to Doris.

WITH user_category_agg AS (
    SELECT
        fk_glusr_usr_id,
        category_type,
        COUNT(*)                          AS listing_count,
        COUNT(DISTINCT glusr_premium_mcat_id) AS mcat_count,
        MAX(last_modified_date)           AS last_update
    FROM poc.glusr_premium_listing
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
