-- GQ04 — GROUP BY high-cardinality column (pc_item_image_glusr_id, seller IDs)
-- Expected to have thousands of distinct values; likely to stress memory on 8 GB VM.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    pc_item_image_glusr_id,
    COUNT(*)                                                    AS image_count,
    COUNT(DISTINCT fk_pc_item_id)                               AS distinct_items,
    SUM(CASE WHEN pc_item_img_status = 'A' THEN 1 ELSE 0 END)  AS active_count,
    SUM(CASE WHEN pc_item_img_status = 'I' THEN 1 ELSE 0 END)  AS inactive_count,
    MAX(pc_item_image_update_date)                              AS last_activity
FROM read_csv_auto(
    's3://<GCS_PC_ITEM_IMAGE_PREFIX>',
    header = true,
    null_padding = true,
    columns = {
        'pc_item_image_id': 'BIGINT',
        'fk_pc_item_id': 'BIGINT',
        'pc_item_image_updatedby': 'VARCHAR',
        'pc_item_image_update_date': 'TIMESTAMP',
        'pc_item_image_original': 'VARCHAR',
        'pc_item_img_status': 'VARCHAR',
        'fk_pc_item_doc_id': 'BIGINT',
        'pc_item_img_doc_order': 'BIGINT',
        'pc_item_image_original_flag': 'BIGINT',
        'pc_item_image_125x125_flag': 'BIGINT',
        'pc_item_image_250x250_flag': 'BIGINT',
        'pc_item_image_500x500_flag': 'BIGINT',
        'pc_item_image_1000x1000_flag': 'BIGINT',
        'pc_item_image_glusr_id': 'BIGINT',
        'pc_item_image_original_width': 'INTEGER',
        'pc_item_image_original_height': 'INTEGER',
        'pc_item_image_125x125_width': 'INTEGER',
        'pc_item_image_125x125_height': 'INTEGER',
        'pc_item_image_250x250_width': 'INTEGER',
        'pc_item_image_250x250_height': 'INTEGER',
        'pc_item_image_500x500_width': 'INTEGER',
        'pc_item_image_500x500_height': 'INTEGER',
        'pc_item_image_125x125': 'VARCHAR',
        'pc_item_image_250x250': 'VARCHAR',
        'pc_item_image_500x500': 'VARCHAR',
        'fk_pc_item_img_rejection_code': 'INTEGER',
        'pc_item_image_1000x1000': 'VARCHAR',
        'pc_item_image_1000x1000_width': 'INTEGER',
        'pc_item_image_1000x1000_height': 'INTEGER',
        'pc_item_image_2000x2000': 'VARCHAR',
        'pc_item_image_2000x2000_width': 'INTEGER',
        'pc_item_image_2000x2000_height': 'INTEGER'
    }
)
GROUP BY pc_item_image_glusr_id
ORDER BY image_count DESC
LIMIT 1000
