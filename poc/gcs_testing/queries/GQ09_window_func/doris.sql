-- GQ09 — Window function: rank sellers within each status bucket by image count
-- Two-level CTE: first aggregates per (glusr_id, status), then applies window functions.
-- Memory-intensive; requires buffering the intermediate aggregate result.
-- Dialect: Apache Doris (s3() TVF)

WITH seller_status_agg AS (
    SELECT
        pc_item_image_glusr_id,
        pc_item_img_status,
        COUNT(*)                       AS image_count,
        COUNT(DISTINCT fk_pc_item_id)  AS item_count,
        MAX(pc_item_image_update_date) AS last_update
    FROM s3(
        "uri"              = "s3://<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>",
        "s3.endpoint"      = "https://storage.googleapis.com",
        "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
        "s3.secret_key"    = "<GCS_HMAC_SECRET>",
        "format"           = "csv",
        "column_separator" = ",",
        "columns"          = "pc_item_image_id BIGINT, fk_pc_item_id BIGINT, pc_item_image_original_width INT, pc_item_image_original_height INT, pc_item_image_125x125_width INT, pc_item_image_125x125_height INT, pc_item_image_250x250_width INT, pc_item_image_250x250_height INT, pc_item_image_500x500_width INT, pc_item_image_500x500_height INT, pc_item_image_original VARCHAR(350), pc_item_image_125x125 VARCHAR(350), pc_item_image_250x250 VARCHAR(350), pc_item_image_500x500 VARCHAR(350), pc_item_image_accessed_by INT, pc_item_image_updatedby VARCHAR(255), pc_item_image_updatedby_id BIGINT, pc_item_image_updatescreen VARCHAR(255), pc_item_image_ip VARCHAR(100), pc_item_image_ip_country VARCHAR(40), pc_item_image_update_date DATETIME, pc_item_image_hist_comments VARCHAR(1000), pc_item_image_updatedby_url VARCHAR(255), pc_item_image_updby_agency VARCHAR(255), pc_item_img_status VARCHAR(1), fk_pc_item_img_rejection_code INT, fk_pc_item_doc_id BIGINT, pc_item_img_doc_order BIGINT, pc_item_image_1000x1000 VARCHAR(350), pc_item_image_1000x1000_width INT, pc_item_image_1000x1000_height INT, pc_item_image_glusr_id BIGINT, pc_item_image_2000x2000 VARCHAR(350), pc_item_image_2000x2000_width INT, pc_item_image_2000x2000_height INT"
    )
    WHERE pc_item_image_glusr_id IS NOT NULL
    GROUP BY pc_item_image_glusr_id, pc_item_img_status
)
SELECT
    pc_item_image_glusr_id,
    pc_item_img_status,
    image_count,
    item_count,
    ROW_NUMBER() OVER (
        PARTITION BY pc_item_img_status
        ORDER BY image_count DESC
    )                                  AS rank_within_status,
    SUM(image_count) OVER (
        PARTITION BY pc_item_img_status
        ORDER BY image_count DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                  AS running_image_total
FROM seller_status_agg
ORDER BY pc_item_img_status, rank_within_status
LIMIT 500
