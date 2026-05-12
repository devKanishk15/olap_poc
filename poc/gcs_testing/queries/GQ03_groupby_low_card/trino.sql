-- GQ03 — GROUP BY low-cardinality column (category_type)
-- Dialect: Trino 481

SELECT
    category_type,
    COUNT(*)                               AS total_listings,
    COUNT(DISTINCT fk_glusr_usr_id)        AS distinct_users,
    COUNT(DISTINCT glusr_premium_mcat_id)  AS distinct_mcats,
    MIN(glusr_premium_added_date)          AS earliest_listing,
    MAX(last_modified_date)                AS latest_modified
FROM poc.glusr_premium_listing
GROUP BY category_type
ORDER BY total_listings DESC
