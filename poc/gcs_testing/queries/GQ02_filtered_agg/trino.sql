-- GQ02 — Filtered aggregate: only enabled listings
-- Dialect: Trino 481

SELECT
    COUNT(*)                               AS total_enabled,
    COUNT(DISTINCT fk_glusr_usr_id)        AS distinct_users,
    COUNT(DISTINCT glusr_premium_mcat_id)  AS distinct_mcats,
    MIN(glusr_premium_added_date)          AS earliest_enabled,
    MAX(last_modified_date)                AS latest_modified
FROM poc.glusr_premium_listing
WHERE glusr_premium_enable = '1'
