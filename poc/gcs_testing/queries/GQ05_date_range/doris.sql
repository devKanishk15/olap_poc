-- GQ05 — Date range filter on pc_item_image_update_date (one quarter: 2024-Q1)
-- Measures how each engine handles a time-bounded scan over an unpartitioned CSV.
-- All rows are scanned (no skip index in CSV); filter is applied post-read.
-- Adjust the date range to match actual data if needed.
-- Dialect: Apache Doris (s3() TVF)

SELECT
    CAST(pc_item_image_update_date AS DATE) AS update_day,
    pc_item_img_status,
    COUNT(*)                                AS images_updated,
    COUNT(DISTINCT pc_item_image_glusr_id)  AS active_sellers
FROM s3(
    "uri"              = "s3://<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>",
    "s3.endpoint"      = "https://storage.googleapis.com",
    "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
    "s3.secret_key"    = "<GCS_HMAC_SECRET>",
    "format"           = "csv",
    "column_separator" = ",",
    "columns"          = "pc_item_image_id BIGINT, fk_pc_item_id BIGINT, pc_item_image_original_width INT, pc_item_image_original_height INT, pc_item_image_125x125_width INT, pc_item_image_125x125_height INT, pc_item_image_250x250_width INT, pc_item_image_250x250_height INT, pc_item_image_500x500_width INT, pc_item_image_500x500_height INT, pc_item_image_original VARCHAR(350), pc_item_image_125x125 VARCHAR(350), pc_item_image_250x250 VARCHAR(350), pc_item_image_500x500 VARCHAR(350), pc_item_image_accessed_by INT, pc_item_image_updatedby VARCHAR(255), pc_item_image_updatedby_id BIGINT, pc_item_image_updatescreen VARCHAR(255), pc_item_image_ip VARCHAR(100), pc_item_image_ip_country VARCHAR(40), pc_item_image_update_date DATETIME, pc_item_image_hist_comments VARCHAR(1000), pc_item_image_updatedby_url VARCHAR(255), pc_item_image_updby_agency VARCHAR(255), pc_item_img_status VARCHAR(1), fk_pc_item_img_rejection_code INT, fk_pc_item_doc_id BIGINT, pc_item_img_doc_order BIGINT, pc_item_image_1000x1000 VARCHAR(350), pc_item_image_1000x1000_width INT, pc_item_image_1000x1000_height INT, pc_item_image_glusr_id BIGINT, pc_item_image_2000x2000 VARCHAR(350), pc_item_image_2000x2000_width INT, pc_item_image_2000x2000_height INT"
)
WHERE pc_item_image_update_date BETWEEN '2024-01-01 00:00:00' AND '2024-03-31 23:59:59'
GROUP BY 1, 2
ORDER BY 1 ASC, images_updated DESC
