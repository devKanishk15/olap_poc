-- GQ07 — String LIKE scan on keyword and comment columns
-- Dialect: Trino 481

SELECT
    category_type,
    COUNT(*)                               AS matched_listings,
    COUNT(DISTINCT fk_glusr_usr_id)        AS users_with_match
FROM poc.glusr_premium_listing
WHERE
    pl_kwrd_term_upper              LIKE '%PREMIUM%'
    OR glusr_premium_hist_comments  LIKE '%approved%'
    OR glusr_premium_updatedby_url  LIKE '%http%'
GROUP BY category_type
ORDER BY matched_listings DESC
