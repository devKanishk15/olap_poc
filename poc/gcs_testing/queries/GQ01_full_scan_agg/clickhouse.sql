-- GQ01 — Full scan + aggregate over pc_item_image CSV in GCS
-- Tests raw GCS-to-engine I/O throughput (~70 GB CSV).
-- No filter; forces a complete file scan.
-- Dialect: ClickHouse (s3() table function, CSVWithNames)
-- Dialect differences vs Doris/DuckDB:
--   COUNT(*) → count()
--   COUNT(DISTINCT x) → uniqExact(x)
--   SUM(CASE WHEN ...) → countIf(...)
--   Uses CSVWithNames (header row provides column names; all types inferred as String)
--   No trailing semicolon — runner appends FORMAT JSON

SELECT
    count()                                         AS total_images,
    uniqExact(fk_pc_item_id)                        AS distinct_items,
    uniqExact(pc_item_image_glusr_id)               AS distinct_sellers,
    min(pc_item_image_update_date)                  AS earliest_update,
    max(pc_item_image_update_date)                  AS latest_update,
    countIf(pc_item_img_status = 'A')               AS active_count
FROM s3(
    'https://storage.googleapis.com/<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>',
    '<GCS_HMAC_ACCESS_KEY>',
    '<GCS_HMAC_SECRET>',
    'CSVWithNames'
)
