-- Q07 — Self-join / small dimension join
-- Dialect differences:
--   ClickHouse CTE syntax is identical (WITH ... AS (...))
--   uniqExact() for distinct count
--   ClickHouse will automatically use broadcast join for the tiny CTE side
-- Dialect: ClickHouse

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
    count()                              AS events,
    sum(ef.revenue)                      AS total_revenue,
    avg(ef.order_total)                  AS avg_order_value,
    uniqExact(ef.user_id)                AS distinct_users
FROM poc.event_fact AS ef
INNER JOIN channel_labels AS cl
    ON ef.campaign_channel = cl.campaign_channel
WHERE
    ef.event_date BETWEEN '2024-01-01' AND '2024-01-30'
    AND ef.is_bot = false
GROUP BY cl.label, ef.event_type
ORDER BY total_revenue DESC
LIMIT 50;
