-- GQ09 — Window function: rank sellers within each status bucket by image count
-- Two-level CTE: first aggregates per (glusr_id, status), then applies window functions.
-- Memory-intensive; requires buffering the intermediate aggregate result.
-- Dialect: ClickHouse (s3() table function, CSV + schema string)
-- Dialect differences vs Doris/DuckDB:
--   IS NOT NULL → isNotNull()
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   Window function syntax is ANSI-compatible (ClickHouse 21.3+)
--   LIMIT 500 is important: ClickHouse materialises the full window frame before LIMIT
--   No trailing semicolon — runner appends FORMAT JSON

WITH seller_status_agg AS (
    SELECT
        pc_item_image_glusr_id,
        pc_item_img_status,
        count()                        AS image_count,
        uniqExact(fk_pc_item_id)      AS item_count,
        max(pc_item_image_update_date) AS last_update
    FROM s3(
        'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>',
        '<GCS_HMAC_ACCESS_KEY>',
        '<GCS_HMAC_SECRET>',
        'CSV',
        'pc_item_image_id UInt64, fk_pc_item_id UInt64, pc_item_image_original_width UInt32, pc_item_image_original_height UInt32, pc_item_image_125x125_width UInt32, pc_item_image_125x125_height UInt32, pc_item_image_250x250_width UInt32, pc_item_image_250x250_height UInt32, pc_item_image_500x500_width UInt32, pc_item_image_500x500_height UInt32, pc_item_image_original String, pc_item_image_125x125 String, pc_item_image_250x250 String, pc_item_image_500x500 String, pc_item_image_accessed_by UInt8, pc_item_image_updatedby String, pc_item_image_updatedby_id UInt64, pc_item_image_updatescreen String, pc_item_image_ip String, pc_item_image_ip_country String, pc_item_image_update_date DateTime, pc_item_image_hist_comments String, pc_item_image_updatedby_url String, pc_item_image_updby_agency String, pc_item_img_status String, fk_pc_item_img_rejection_code UInt32, fk_pc_item_doc_id UInt64, pc_item_img_doc_order UInt64, pc_item_image_1000x1000 String, pc_item_image_1000x1000_width UInt32, pc_item_image_1000x1000_height UInt32, pc_item_image_glusr_id UInt64, pc_item_image_2000x2000 String, pc_item_image_2000x2000_width UInt32, pc_item_image_2000x2000_height UInt32'
    )
    WHERE isNotNull(pc_item_image_glusr_id)
    GROUP BY pc_item_image_glusr_id, pc_item_img_status
)
SELECT
    pc_item_image_glusr_id,
    pc_item_img_status,
    image_count,
    item_count,
    row_number() OVER (
        PARTITION BY pc_item_img_status
        ORDER BY image_count DESC
    )                                  AS rank_within_status,
    sum(image_count) OVER (
        PARTITION BY pc_item_img_status
        ORDER BY image_count DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                  AS running_image_total
FROM seller_status_agg
ORDER BY pc_item_img_status, rank_within_status
LIMIT 500
