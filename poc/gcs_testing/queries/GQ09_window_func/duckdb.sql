-- GQ09 — Window function: rank users within each category_type by listing count
-- Two-level CTE: first aggregates per (fk_glusr_usr_id, category_type), then applies window functions.
-- Memory-intensive; requires buffering the intermediate aggregate result.
-- Dialect: DuckDB (read_csv_auto via httpfs)
-- NOTE: Runner injects LOAD httpfs + SET s3_* credentials before executing this file.

WITH user_category_agg AS (
    SELECT
        fk_glusr_usr_id,
        category_type,
        COUNT(*)                              AS listing_count,
        COUNT(DISTINCT glusr_premium_mcat_id) AS mcat_count,
        MAX(last_modified_date)               AS last_update
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
    WHERE fk_glusr_usr_id IS NOT NULL
    GROUP BY fk_glusr_usr_id, category_type
)
SELECT
    fk_glusr_usr_id,
    category_type,
    listing_count,
    mcat_count,
    ROW_NUMBER() OVER (
        PARTITION BY category_type
        ORDER BY listing_count DESC
    )                                     AS rank_within_category,
    SUM(listing_count) OVER (
        PARTITION BY category_type
        ORDER BY listing_count DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                     AS running_listing_total
FROM user_category_agg
ORDER BY category_type, rank_within_category
LIMIT 500
