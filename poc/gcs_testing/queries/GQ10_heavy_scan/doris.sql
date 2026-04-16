-- GQ10 — Heavy multi-column scan (deliberate memory pressure / spill candidate)
-- Reads all 6 URL varchar(350) columns + hist_comments varchar(1000).
-- Two-level CTE with 3-column GROUP BY; designed to exhaust 8 GB RAM and trigger spill.
-- Dialect: Apache Doris (s3() TVF)

WITH per_seller_status AS (
    SELECT
        pc_item_image_glusr_id,
        pc_item_img_status,
        pc_item_image_ip_country,
        COUNT(*)                                                            AS image_count,
        COUNT(DISTINCT fk_pc_item_id)                                       AS item_count,
        SUM(CASE WHEN pc_item_image_hist_comments IS NOT NULL THEN 1 ELSE 0 END) AS has_comment_count,
        SUM(CASE WHEN pc_item_image_original   LIKE '%http%' THEN 1 ELSE 0 END)  AS original_http_count,
        SUM(CASE WHEN pc_item_image_500x500    LIKE '%http%' THEN 1 ELSE 0 END)  AS thumb_500_http_count,
        MAX(pc_item_image_update_date)                                      AS last_update
    FROM s3(
        "uri"              = "s3://<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>",
        "s3.endpoint"      = "https://storage.googleapis.com",
        "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
        "s3.secret_key"    = "<GCS_HMAC_SECRET>",
        "format"           = "csv",
        "column_separator" = ",",
        "columns"          = "pc_item_image_id BIGINT, fk_pc_item_id BIGINT, pc_item_image_original_width INT, pc_item_image_original_height INT, pc_item_image_125x125_width INT, pc_item_image_125x125_height INT, pc_item_image_250x250_width INT, pc_item_image_250x250_height INT, pc_item_image_500x500_width INT, pc_item_image_500x500_height INT, pc_item_image_original VARCHAR(350), pc_item_image_125x125 VARCHAR(350), pc_item_image_250x250 VARCHAR(350), pc_item_image_500x500 VARCHAR(350), pc_item_image_accessed_by INT, pc_item_image_updatedby VARCHAR(255), pc_item_image_updatedby_id BIGINT, pc_item_image_updatescreen VARCHAR(255), pc_item_image_ip VARCHAR(100), pc_item_image_ip_country VARCHAR(40), pc_item_image_update_date DATETIME, pc_item_image_hist_comments VARCHAR(1000), pc_item_image_updatedby_url VARCHAR(255), pc_item_image_updby_agency VARCHAR(255), pc_item_img_status VARCHAR(1), fk_pc_item_img_rejection_code INT, fk_pc_item_doc_id BIGINT, pc_item_img_doc_order BIGINT, pc_item_image_1000x1000 VARCHAR(350), pc_item_image_1000x1000_width INT, pc_item_image_1000x1000_height INT, pc_item_image_glusr_id BIGINT, pc_item_image_2000x2000 VARCHAR(350), pc_item_image_2000x2000_width INT, pc_item_image_2000x2000_height INT"
    )
    GROUP BY pc_item_image_glusr_id, pc_item_img_status, pc_item_image_ip_country
)
SELECT
    pc_item_img_status,
    pc_item_image_ip_country,
    COUNT(DISTINCT pc_item_image_glusr_id) AS sellers,
    SUM(image_count)                       AS total_images,
    SUM(item_count)                        AS total_items,
    SUM(has_comment_count)                 AS images_with_comments,
    SUM(original_http_count)               AS original_http_total,
    SUM(thumb_500_http_count)              AS thumb_500_http_total,
    AVG(image_count)                       AS avg_images_per_seller_status
FROM per_seller_status
GROUP BY pc_item_img_status, pc_item_image_ip_country
ORDER BY total_images DESC
LIMIT 200
