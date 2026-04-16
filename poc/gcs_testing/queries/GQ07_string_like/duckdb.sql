-- GQ07 — String LIKE scan on URL columns and hist_comments
-- Stresses string scanning on the widest columns (varchar(350) URLs + varchar(1000) comments).
-- High I/O cost per row due to column widths.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    pc_item_img_status,
    COUNT(*)                               AS matched_images,
    COUNT(DISTINCT pc_item_image_glusr_id) AS sellers_with_match
FROM read_csv_auto(
    's3://<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>',
    header = true,
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
WHERE
    pc_item_image_original   LIKE '%/images/%'
    OR pc_item_image_500x500 LIKE '%cdn%'
    OR pc_item_image_hist_comments LIKE '%approved%'
GROUP BY pc_item_img_status
ORDER BY matched_images DESC
