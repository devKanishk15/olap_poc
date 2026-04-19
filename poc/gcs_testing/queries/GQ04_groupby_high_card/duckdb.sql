-- GQ04 — GROUP BY high-cardinality column (fk_glusr_usr_id, user IDs)
-- Expected to have thousands of distinct users; likely to stress memory on 8 GB VM.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

SELECT
    fk_glusr_usr_id,
    COUNT(*)                                                              AS listing_count,
    COUNT(DISTINCT glusr_premium_mcat_id)                                 AS distinct_mcats,
    SUM(CASE WHEN glusr_premium_enable = '1' THEN 1 ELSE 0 END)          AS enabled_count,
    SUM(CASE WHEN flag_premium_listing = '1' THEN 1 ELSE 0 END)          AS premium_count,
    MAX(last_modified_date)                                               AS last_activity
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
GROUP BY fk_glusr_usr_id
ORDER BY listing_count DESC
LIMIT 1000
