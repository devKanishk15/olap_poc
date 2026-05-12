-- GQ01 — Full scan + aggregate over glusr_premium_listing CSV in GCS
-- Dialect: Trino 481
-- Trino reads from the gcs_hive catalog external table (pre-registered via trino_gcs_ddl.sql).
-- No TVF — credentials and location are configured in gcs_hive.properties.

SELECT
    COUNT(*)                                                              AS total_listings,
    COUNT(DISTINCT fk_glusr_usr_id)                                       AS distinct_users,
    COUNT(DISTINCT glusr_premium_mcat_id)                                 AS distinct_mcats,
    MIN(glusr_premium_added_date)                                         AS earliest_listing,
    MAX(glusr_premium_added_date)                                         AS latest_listing,
    SUM(CASE WHEN glusr_premium_enable = '1' THEN 1 ELSE 0 END)          AS enabled_count
FROM poc.glusr_premium_listing
