-- Q13 — Multi-dimension GROUP BY (campaign attribution analysis)
-- Dialect differences:
--   countIf() replaces SUM(CASE WHEN ...) — ClickHouse idiom, same semantics
--   nullIf() replaces NULLIF() — same semantics, different capitalisation convention
--   uniqExact() for distinct users
-- Dialect: ClickHouse

SELECT
    product_category_l1,
    campaign_channel,
    ab_variant,
    device_type,
    count()                                             AS events,
    uniqExact(user_id)                                  AS distinct_users,
    sum(revenue)                                        AS total_revenue,
    sum(order_total)                                    AS gross_order_value,
    sum(discount_amount)                                AS total_discounts,
    avg(order_total)                                    AS avg_order_value,
    countIf(event_type = 'purchase')                    AS purchases,
    countIf(event_type = 'add_to_cart')                 AS add_to_carts,
    round(
        100.0 * countIf(event_type = 'purchase') /
        nullIf(countIf(event_type = 'add_to_cart'), 0),
        2
    )                                                   AS cart_to_purchase_pct
FROM poc.event_fact
WHERE
    is_bot = false
    AND isNotNull(product_category_l1)
    AND isNotNull(campaign_channel)
    AND isNotNull(ab_variant)
GROUP BY
    product_category_l1,
    campaign_channel,
    ab_variant,
    device_type
ORDER BY total_revenue DESC;
