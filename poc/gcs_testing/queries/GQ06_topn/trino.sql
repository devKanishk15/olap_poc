-- GQ06 — TOP-N users by listing count (LIMIT 100)
-- Dialect: Trino 481

SELECT
    fk_glusr_usr_id,
    COUNT(*)                               AS total_listings,
    COUNT(DISTINCT glusr_premium_mcat_id)  AS distinct_mcats,
    SUM(CASE WHEN glusr_premium_enable = '1' THEN 1 ELSE 0 END) AS enabled_listings,
    MAX(last_modified_date)                AS last_update
FROM poc.glusr_premium_listing
WHERE fk_glusr_usr_id IS NOT NULL
GROUP BY fk_glusr_usr_id
ORDER BY total_listings DESC
LIMIT 100
