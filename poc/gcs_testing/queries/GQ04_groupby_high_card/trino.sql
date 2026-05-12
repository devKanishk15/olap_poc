-- GQ04 — GROUP BY high-cardinality column (fk_glusr_usr_id)
-- Dialect: Trino 481

SELECT
    fk_glusr_usr_id,
    COUNT(*)                                                              AS listing_count,
    COUNT(DISTINCT glusr_premium_mcat_id)                                 AS distinct_mcats,
    SUM(CASE WHEN glusr_premium_enable = '1' THEN 1 ELSE 0 END)          AS enabled_count,
    SUM(CASE WHEN flag_premium_listing = '1' THEN 1 ELSE 0 END)          AS premium_count,
    MAX(last_modified_date)                                               AS last_activity
FROM poc.glusr_premium_listing
GROUP BY fk_glusr_usr_id
ORDER BY listing_count DESC
LIMIT 1000
