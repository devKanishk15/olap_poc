-- GQ07 — String LIKE scan on keyword and comment columns
-- Stresses string scanning on wide columns: pl_kwrd_term_upper (varchar 500),
-- glusr_premium_hist_comments (varchar 1000), glusr_premium_updatedby_url (varchar 255).
-- High I/O cost per row due to column widths.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    category_type,
    COUNT(*)                               AS matched_listings,
    COUNT(DISTINCT fk_glusr_usr_id)        AS users_with_match
FROM read_csv_auto(
    's3://<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    header = true,
    null_padding = true,
    columns = {
        'glusr_premium_listing_id': 'BIGINT',
        'fk_glusr_usr_id': 'BIGINT',
        'glusr_premium_mcat_id': 'BIGINT',
        'glusr_premium_city_id': 'BIGINT',
        'flag_premium_listing': 'VARCHAR',
        'fk_service_id': 'BIGINT',
        'fk_cust_to_serv_id': 'BIGINT',
        'pl_kwrd_term_upper': 'VARCHAR',
        'glusr_premium_enable': 'VARCHAR',
        'glusr_premium_added_date': 'TIMESTAMP',
        'last_modified_date': 'TIMESTAMP',
        'glusr_premium_updatedby_id': 'BIGINT',
        'glusr_premium_updatedby': 'VARCHAR',
        'glusr_premium_updatescreen': 'VARCHAR',
        'glusr_premium_ip': 'VARCHAR',
        'glusr_premium_ip_country': 'VARCHAR',
        'glusr_premium_hist_comments': 'VARCHAR',
        'glusr_premium_updatedby_url': 'VARCHAR',
        'category_type': 'VARCHAR',
        'location_type': 'VARCHAR',
        'location_iso': 'VARCHAR',
        'category_location_credit_value': 'DOUBLE'
    }
)
WHERE
    pl_kwrd_term_upper              LIKE '%PREMIUM%'
    OR glusr_premium_hist_comments  LIKE '%approved%'
    OR glusr_premium_updatedby_url  LIKE '%http%'
GROUP BY category_type
ORDER BY matched_listings DESC
