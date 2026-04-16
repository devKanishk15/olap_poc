-- GQ01 — Full scan + aggregate over pc_item_image CSV in GCS
-- Tests raw GCS-to-engine I/O throughput (~70 GB CSV).
-- No filter; forces a complete file scan.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    COUNT(*)                                                    AS total_images,
    COUNT(DISTINCT fk_pc_item_id)                               AS distinct_items,
    COUNT(DISTINCT pc_item_image_glusr_id)                      AS distinct_sellers,
    MIN(pc_item_image_update_date)                              AS earliest_update,
    MAX(pc_item_image_update_date)                              AS latest_update,
    SUM(CASE WHEN pc_item_img_status = 'A' THEN 1 ELSE 0 END)  AS active_count
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
        'pc_item_image_glusr_id': 'BIGINT',
        'pc_item_image_2000x2000': 'VARCHAR',
        'pc_item_image_2000x2000_width': 'INTEGER',
        'pc_item_image_2000x2000_height': 'INTEGER'
    }
)
