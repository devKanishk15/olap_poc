-- GQ05 — Date range filter on pc_item_image_update_date (one quarter: 2024-Q1)
-- Measures how each engine handles a time-bounded scan over an unpartitioned CSV.
-- All rows are scanned (no skip index in CSV); filter is applied post-read.
-- Adjust the date range to match actual data if needed.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    CAST(pc_item_image_update_date AS DATE) AS update_day,
    pc_item_img_status,
    COUNT(*)                                AS images_updated,
    COUNT(DISTINCT pc_item_image_glusr_id)  AS active_sellers
FROM read_csv_auto(
    's3://<GCS_PC_ITEM_IMAGE_PREFIX>',
    header = true,
    null_padding = true,
    columns = {
        'pc_item_image_id': 'BIGINT',
        'fk_pc_item_id': 'BIGINT',
        'pc_item_image_original_width': 'INTEGER',
        'pc_item_image_original_height': 'INTEGER',
        'pc_item_image_125x125_width': 'INTEGER',
        'pc_item_image_125x125_height': 'INTEGER',
        'pc_item_image_250x250_width': 'INTEGER',
        'pc_item_image_250x250_height': 'INTEGER',
        'pc_item_image_500x500_width': 'INTEGER',
        'pc_item_image_500x500_height': 'INTEGER',
        'pc_item_image_original': 'VARCHAR',
        'pc_item_image_125x125': 'VARCHAR',
        'pc_item_image_250x250': 'VARCHAR',
        'pc_item_image_500x500': 'VARCHAR',
        'pc_item_image_accessed_by': 'INTEGER',
        'pc_item_image_updatedby': 'VARCHAR',
        'pc_item_image_updatedby_id': 'BIGINT',
        'pc_item_image_updatescreen': 'VARCHAR',
        'pc_item_image_ip': 'VARCHAR',
        'pc_item_image_ip_country': 'VARCHAR',
        'pc_item_image_update_date': 'TIMESTAMP',
        'pc_item_image_hist_comments': 'VARCHAR',
        'pc_item_image_updatedby_url': 'VARCHAR',
        'pc_item_image_updby_agency': 'VARCHAR',
        'pc_item_img_status': 'VARCHAR',
        'fk_pc_item_img_rejection_code': 'INTEGER',
        'fk_pc_item_doc_id': 'BIGINT',
        'pc_item_img_doc_order': 'BIGINT',
        'pc_item_image_1000x1000': 'VARCHAR',
        'pc_item_image_1000x1000_width': 'INTEGER',
        'pc_item_image_1000x1000_height': 'INTEGER',
        'pc_item_image_glusr_id': 'BIGINT'
    }
)
WHERE pc_item_image_update_date BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-03-31 23:59:59'
GROUP BY 1, 2
ORDER BY 1 ASC, images_updated DESC
