-- Q07 — Self-join / small dimension join
-- Dialect differences: None — DuckDB supports CTE joins natively
-- DuckDB auto-broadcasts the small CTE side
-- Dialect: DuckDB

WITH channel_labels AS (
    SELECT 'email'             AS campaign_channel, 'Email Marketing'    AS label UNION ALL
    SELECT 'paid_search',                            'SEM / PPC'                  UNION ALL
    SELECT 'organic',                                'Organic Search'             UNION ALL
    SELECT 'direct',                                 'Direct Traffic'             UNION ALL
    SELECT 'social',                                 'Social Media'               UNION ALL
    SELECT 'affiliate',                              'Affiliate'                  UNION ALL
    SELECT 'referral',                               'Referral'                   UNION ALL
    SELECT 'display',                                'Display Ads'                UNION ALL
    SELECT 'push_notification',                      'Push Notifications'
)
SELECT
    cl.label                             AS channel_label,
    ef.event_type,
    COUNT(*)                             AS events,
    SUM(ef.revenue)                      AS total_revenue,
    AVG(ef.order_total)                  AS avg_order_value,
    COUNT(DISTINCT ef.user_id)           AS distinct_users
FROM poc.event_fact ef
INNER JOIN channel_labels cl
    ON ef.campaign_channel = cl.campaign_channel
WHERE
    ef.event_date BETWEEN '2024-01-01' AND '2024-01-30'
    AND ef.is_bot = FALSE
GROUP BY cl.label, ef.event_type
ORDER BY total_revenue DESC
LIMIT 50;
