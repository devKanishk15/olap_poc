-- GQ10 — Heavy multi-column scan (deliberate memory pressure / spill candidate)
-- Reads all URL varchar(350) columns across the full CSV.
-- Two-level CTE with 2-column GROUP BY; designed to exhaust 8 GB RAM and trigger spill.
-- Note: ip_country and hist_comments absent from this CSV version;
--       has_comment_count replaced with has_1000x1000_count.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials + SET memory_limit = '6GB'
--       + SET temp_directory before executing this file. Spill is expected.

WITH per_seller_status AS (
    SELECT
        pc_item_image_glusr_id,
        pc_item_img_status,
        COUNT(*)                                                                        AS image_count,
        COUNT(DISTINCT fk_pc_item_id)                                                   AS item_count,
        SUM(CASE WHEN pc_item_image_1000x1000 IS NOT NULL
                  AND pc_item_image_1000x1000 <> '' THEN 1 ELSE 0 END)                 AS has_1000_count,
        SUM(CASE WHEN pc_item_image_original  LIKE '%http%' THEN 1 ELSE 0 END)          AS original_http_count,
        SUM(CASE WHEN pc_item_image_500x500   LIKE '%http%' THEN 1 ELSE 0 END)          AS thumb_500_http_count,
        MAX(pc_item_image_update_date)                                                  AS last_update
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
    GROUP BY pc_item_image_glusr_id, pc_item_img_status
)
SELECT
    pc_item_img_status,
    COUNT(DISTINCT pc_item_image_glusr_id) AS sellers,
    SUM(image_count)                       AS total_images,
    SUM(item_count)                        AS total_items,
    SUM(has_1000_count)                    AS images_with_1000x1000,
    SUM(original_http_count)               AS original_http_total,
    SUM(thumb_500_http_count)              AS thumb_500_http_total,
    AVG(image_count)                       AS avg_images_per_seller_status
FROM per_seller_status
GROUP BY pc_item_img_status
ORDER BY total_images DESC
LIMIT 200
