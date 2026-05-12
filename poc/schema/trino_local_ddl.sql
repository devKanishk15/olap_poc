-- =============================================================================
-- Trino DDL — local_hive catalog (local Parquet files)
-- Run via: make schema-trino-local
--          OR: docker exec -i trino trino --catalog local_hive --schema poc < this_file
--
-- Creates an EXTERNAL table over the hive-partitioned Parquet files at
-- /opt1/olap_poc/data/event_fact/event_date=YYYY-MM-DD/part-*.parquet
-- The partition column (event_date) is read from the directory name.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS poc;

DROP TABLE IF EXISTS poc.event_fact;

-- External table pointing at local Parquet (bind-mounted read-only into container).
-- event_date listed last because it is the partition column (Hive convention).
CREATE TABLE poc.event_fact (
    event_id                BIGINT,
    event_ts                TIMESTAMP(6) ,
    session_id              VARCHAR,
    user_id                 BIGINT,
    device_id               VARCHAR,
    event_type              VARCHAR,
    event_subtype           VARCHAR,
    page_id                 INTEGER,
    page_name               VARCHAR,
    referrer_url            VARCHAR,
    campaign_id             VARCHAR,
    campaign_channel        VARCHAR,
    ab_variant              VARCHAR,
    country_code            VARCHAR,
    region                  VARCHAR,
    city                    VARCHAR,
    latitude                DOUBLE,
    longitude               DOUBLE,
    ip_address              VARCHAR,
    user_agent              VARCHAR,
    os_family               VARCHAR,
    browser_family          VARCHAR,
    device_type             VARCHAR,
    screen_width            SMALLINT,
    screen_height           SMALLINT,
    viewport_width          SMALLINT,
    viewport_height         SMALLINT,
    product_id              INTEGER,
    product_name            VARCHAR,
    product_category_l1     VARCHAR,
    product_category_l2     VARCHAR,
    product_price           DECIMAL(12, 2),
    quantity                SMALLINT,
    order_id                BIGINT,
    order_total             DECIMAL(14, 2),
    discount_amount         DECIMAL(10, 2),
    coupon_code             VARCHAR,
    revenue                 DECIMAL(14, 4),
    duration_ms             INTEGER,
    scroll_depth_pct        TINYINT,
    click_x                 SMALLINT,
    click_y                 SMALLINT,
    is_bot                  BOOLEAN,
    is_authenticated        BOOLEAN,
    is_first_visit          BOOLEAN,
    experiment_id           INTEGER,
    server_id               SMALLINT,
    load_time_ms            INTEGER,
    ttfb_ms                 SMALLINT,
    error_code              SMALLINT,
    error_message           VARCHAR,
    tag_list                VARCHAR,
    custom_dimensions       VARCHAR,
    raw_payload_size_bytes  INTEGER,
    ingestion_ts            TIMESTAMP(6),
    processing_lag_ms       INTEGER,
    data_version            TINYINT,
    partition_key           INTEGER,
    checksum                BIGINT,
    -- Partition column: derived from directory name event_date=YYYY-MM-DD
    event_date              DATE
)
WITH (
    external_location = 'file:///opt1/olap_poc/data/event_fact',
    format            = 'PARQUET',
    partitioned_by    = ARRAY['event_date']
);

-- Scan the directory and register all partitions in the file metastore.
-- Must run after CREATE TABLE. Safe to re-run (FULL mode reconciles adds/drops).
CALL system.sync_partition_metadata(
    schema_name => 'poc',
    table_name  => 'event_fact',
    mode        => 'FULL'
);

-- Verify
SELECT 'local_hive.poc.event_fact registered — row count:' AS msg, count(*) AS rows
FROM poc.event_fact;
