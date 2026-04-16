-- GQ06 — TOP-N sellers by image count (LIMIT 100)
-- Tests partial-sort / top-heap optimisation; engines should not sort the entire
-- aggregated result before applying LIMIT.
-- Dialect: ClickHouse (s3() table function, CSV + schema string)
-- Dialect differences vs Doris/DuckDB:
--   IS NOT NULL → isNotNull()
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   SUM(CASE WHEN ...) → countIf(...)
--   No trailing semicolon — runner appends FORMAT JSON

SELECT
    pc_item_image_glusr_id,
    count()                                AS total_images,
    uniqExact(fk_pc_item_id)              AS distinct_items,
    countIf(pc_item_img_status = 'A')     AS active_images,
    max(pc_item_image_update_date)        AS last_upload
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>',
    '<GCS_HMAC_SECRET>',
    'CSV',
    'pc_item_image_id UInt64, fk_pc_item_id UInt64, pc_item_image_original_width UInt32, pc_item_image_original_height UInt32, pc_item_image_125x125_width UInt32, pc_item_image_125x125_height UInt32, pc_item_image_250x250_width UInt32, pc_item_image_250x250_height UInt32, pc_item_image_500x500_width UInt32, pc_item_image_500x500_height UInt32, pc_item_image_original String, pc_item_image_125x125 String, pc_item_image_250x250 String, pc_item_image_500x500 String, pc_item_image_accessed_by UInt8, pc_item_image_updatedby String, pc_item_image_updatedby_id UInt64, pc_item_image_updatescreen String, pc_item_image_ip String, pc_item_image_ip_country String, pc_item_image_update_date DateTime, pc_item_image_hist_comments String, pc_item_image_updatedby_url String, pc_item_image_updby_agency String, pc_item_img_status String, fk_pc_item_img_rejection_code UInt32, fk_pc_item_doc_id UInt64, pc_item_img_doc_order UInt64, pc_item_image_1000x1000 String, pc_item_image_1000x1000_width UInt32, pc_item_image_1000x1000_height UInt32, pc_item_image_glusr_id UInt64, pc_item_image_2000x2000 String, pc_item_image_2000x2000_width UInt32, pc_item_image_2000x2000_height UInt32'
)
WHERE isNotNull(pc_item_image_glusr_id)
GROUP BY pc_item_image_glusr_id
ORDER BY total_images DESC
LIMIT 100
