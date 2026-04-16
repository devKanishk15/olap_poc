-- GQ08 — Approximate vs exact distinct count comparison
-- Compares APPROX_COUNT_DISTINCT (HyperLogLog) vs COUNT(DISTINCT) per status bucket.
-- Highlights engines that have native HLL support vs those that materialise the full set.
-- Dialect: Apache Doris (s3() TVF)

SELECT
    pc_item_img_status,
    COUNT(*)                                    AS total_images,
    APPROX_COUNT_DISTINCT(pc_item_image_glusr_id) AS approx_distinct_sellers,
    COUNT(DISTINCT pc_item_image_glusr_id)      AS exact_distinct_sellers,
    APPROX_COUNT_DISTINCT(fk_pc_item_id)        AS approx_distinct_items
FROM s3(
    "uri"              = "s3://<GCS_BUCKET>/<GCS_PC_ITEM_IMAGE_PREFIX>",
    "s3.endpoint"      = "https://storage.googleapis.com",
    "s3.access_key"    = "<GCS_HMAC_ACCESS_KEY>",
    "s3.secret_key"    = "<GCS_HMAC_SECRET>",
    "format"           = "csv",
    "column_separator" = ",",
    "columns"          = "pc_item_image_id BIGINT, fk_pc_item_id BIGINT, pc_item_image_original_width INT, pc_item_image_original_height INT, pc_item_image_125x125_width INT, pc_item_image_125x125_height INT, pc_item_image_250x250_width INT, pc_item_image_250x250_height INT, pc_item_image_500x500_width INT, pc_item_image_500x500_height INT, pc_item_image_original VARCHAR(350), pc_item_image_125x125 VARCHAR(350), pc_item_image_250x250 VARCHAR(350), pc_item_image_500x500 VARCHAR(350), pc_item_image_accessed_by INT, pc_item_image_updatedby VARCHAR(255), pc_item_image_updatedby_id BIGINT, pc_item_image_updatescreen VARCHAR(255), pc_item_image_ip VARCHAR(100), pc_item_image_ip_country VARCHAR(40), pc_item_image_update_date DATETIME, pc_item_image_hist_comments VARCHAR(1000), pc_item_image_updatedby_url VARCHAR(255), pc_item_image_updby_agency VARCHAR(255), pc_item_img_status VARCHAR(1), fk_pc_item_img_rejection_code INT, fk_pc_item_doc_id BIGINT, pc_item_img_doc_order BIGINT, pc_item_image_1000x1000 VARCHAR(350), pc_item_image_1000x1000_width INT, pc_item_image_1000x1000_height INT, pc_item_image_glusr_id BIGINT, pc_item_image_2000x2000 VARCHAR(350), pc_item_image_2000x2000_width INT, pc_item_image_2000x2000_height INT"
)
GROUP BY pc_item_img_status
ORDER BY total_images DESC
