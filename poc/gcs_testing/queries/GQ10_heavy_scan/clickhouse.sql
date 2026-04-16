-- GQ10 — Heavy multi-column scan (deliberate memory pressure / spill candidate)
-- Reads all 6 URL varchar(350) columns + hist_comments varchar(1000).
-- Two-level CTE with 3-column GROUP BY; designed to exhaust 8 GB RAM and trigger spill.
-- Dialect: ClickHouse (s3() table function, CSV + schema string)
-- Dialect differences vs Doris/DuckDB:
--   SUM(CASE WHEN col IS NOT NULL ...) → countIf(isNotNull(col))
--   SUM(CASE WHEN col LIKE ... ) → countIf(like(col, pattern))
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   AVG(x) → avg(x)
--   SETTINGS clause enables spill-to-disk rather than OOM for inner GROUP BY
--   No trailing semicolon — runner appends FORMAT JSON

WITH per_seller_status AS (
    SELECT
        pc_item_image_glusr_id,
        pc_item_img_status,
        pc_item_image_ip_country,
        count()                                                 AS image_count,
        uniqExact(fk_pc_item_id)                               AS item_count,
        countIf(isNotNull(pc_item_image_hist_comments))        AS has_comment_count,
        countIf(like(pc_item_image_original, '%http%'))        AS original_http_count,
        countIf(like(pc_item_image_500x500, '%http%'))         AS thumb_500_http_count,
        max(pc_item_image_update_date)                         AS last_update
    FROM s3(
        'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>',
        '<GCS_HMAC_ACCESS_KEY>',
        '<GCS_HMAC_SECRET>',
        'CSV',
        'pc_item_image_id UInt64, fk_pc_item_id UInt64, pc_item_image_original_width UInt32, pc_item_image_original_height UInt32, pc_item_image_125x125_width UInt32, pc_item_image_125x125_height UInt32, pc_item_image_250x250_width UInt32, pc_item_image_250x250_height UInt32, pc_item_image_500x500_width UInt32, pc_item_image_500x500_height UInt32, pc_item_image_original String, pc_item_image_125x125 String, pc_item_image_250x250 String, pc_item_image_500x500 String, pc_item_image_accessed_by UInt8, pc_item_image_updatedby String, pc_item_image_updatedby_id UInt64, pc_item_image_updatescreen String, pc_item_image_ip String, pc_item_image_ip_country String, pc_item_image_update_date DateTime, pc_item_image_hist_comments String, pc_item_image_updatedby_url String, pc_item_image_updby_agency String, pc_item_img_status String, fk_pc_item_img_rejection_code UInt32, fk_pc_item_doc_id UInt64, pc_item_img_doc_order UInt64, pc_item_image_1000x1000 String, pc_item_image_1000x1000_width UInt32, pc_item_image_1000x1000_height UInt32, pc_item_image_glusr_id UInt64, pc_item_image_2000x2000 String, pc_item_image_2000x2000_width UInt32, pc_item_image_2000x2000_height UInt32'
    )
    GROUP BY pc_item_image_glusr_id, pc_item_img_status, pc_item_image_ip_country
)
SELECT
    pc_item_img_status,
    pc_item_image_ip_country,
    uniqExact(pc_item_image_glusr_id)      AS sellers,
    sum(image_count)                       AS total_images,
    sum(item_count)                        AS total_items,
    sum(has_comment_count)                 AS images_with_comments,
    sum(original_http_count)               AS original_http_total,
    sum(thumb_500_http_count)              AS thumb_500_http_total,
    avg(image_count)                       AS avg_images_per_seller_status
FROM per_seller_status
GROUP BY pc_item_img_status, pc_item_image_ip_country
ORDER BY total_images DESC
LIMIT 200
SETTINGS max_bytes_before_external_group_by = 3000000000
