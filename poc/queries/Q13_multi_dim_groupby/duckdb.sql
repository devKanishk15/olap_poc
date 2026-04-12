-- Q13 — Multi-dimension GROUP BY (campaign attribution analysis)
-- Dialect differences: None — DuckDB is fully ANSI-compatible here
-- Dialect: DuckDB

SELECT
    product_category_l1,
    campaign_channel,
    ab_variant,
    device_type,
    COUNT(*)                                                    AS events,
    COUNT(DISTINCT user_id)                                     AS distinct_users,
    SUM(revenue)                                                AS total_revenue,
    SUM(order_total)                                            AS gross_order_value,
    SUM(discount_amount)                                        AS total_discounts,
    AVG(order_total)                                            AS avg_order_value,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)   AS purchases,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_carts,
    ROUND(
        100.0 * SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END), 0),
        2
    )                                                           AS cart_to_purchase_pct
FROM poc.event_fact
WHERE
    is_bot = FALSE
    AND product_category_l1 IS NOT NULL
    AND campaign_channel IS NOT NULL
    AND ab_variant IS NOT NULL
GROUP BY
    product_category_l1,
    campaign_channel,
    ab_variant,
    device_type
ORDER BY total_revenue DESC;
