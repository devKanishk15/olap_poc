-- GQ08 — Approximate vs exact distinct count comparison
-- Dialect: Trino 481
-- approx_distinct(x) replaces APPROX_COUNT_DISTINCT(x) from Doris.

SELECT
    category_type,
    COUNT(*)                                         AS total_listings,
    approx_distinct(fk_glusr_usr_id)                 AS approx_distinct_users,
    COUNT(DISTINCT fk_glusr_usr_id)                  AS exact_distinct_users,
    approx_distinct(glusr_premium_mcat_id)           AS approx_distinct_mcats
FROM poc.glusr_premium_listing
GROUP BY category_type
ORDER BY total_listings DESC
