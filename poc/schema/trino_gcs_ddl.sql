-- =============================================================================
-- Trino DDL — gcs_hive catalog (GCS Parquet + CSV files)
-- Run via: make schema-trino-gcs
--          OR: docker exec -i trino trino --catalog gcs_hive --schema poc < this_file
--
-- Registers two external tables:
--   poc.event_fact           — GCS Parquet benchmark data (Q01–Q13 in GCS mode)
--   poc.glusr_premium_listing — GCS CSV file (GQ01–GQ10 testing suite)
--
-- GCS credentials are pre-configured in gcs_hive.properties catalog file.
-- Tables use s3:// URIs — the catalog routes them to storage.googleapis.com.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS poc;

-- ---------------------------------------------------------------------------
-- Table 1: event_fact (hive-partitioned Parquet in GCS)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS poc.event_fact;

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
    event_date              DATE
)
WITH (
    -- Replace <GCS_BUCKET> and <GCS_BUCKET_PREFIX> with real values from .env
    -- The install script handles this substitution before running the DDL.
    external_location = 's3://<GCS_BUCKET>/<GCS_BUCKET_PREFIX>/event_fact',
    format            = 'PARQUET',
    partitioned_by    = ARRAY['event_date']
);

CALL system.sync_partition_metadata(
    schema_name => 'poc',
    table_name  => 'event_fact',
    mode        => 'FULL'
);

-- ---------------------------------------------------------------------------
-- Table 2: glusr_premium_listing (CSV in GCS — for GQ01-GQ10 benchmarks)
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS poc.glusr_premium_listing;

-- CSV table with 22 columns (header row is skipped).
CREATE TABLE poc.glusr_premium_listing (
    glusr_premium_listing_id        BIGINT,
    fk_glusr_usr_id                 BIGINT,
    glusr_premium_mcat_id           BIGINT,
    glusr_premium_city_id           BIGINT,
    flag_premium_listing            VARCHAR,
    fk_service_id                   BIGINT,
    fk_cust_to_serv_id              BIGINT,
    pl_kwrd_term_upper              VARCHAR,
    glusr_premium_enable            VARCHAR,
    glusr_premium_added_date        TIMESTAMP(6),
    last_modified_date              TIMESTAMP(6),
    glusr_premium_updatedby_id      BIGINT,
    glusr_premium_updatedby         VARCHAR,
    glusr_premium_updatescreen      VARCHAR,
    glusr_premium_ip                VARCHAR,
    glusr_premium_ip_country        VARCHAR,
    glusr_premium_hist_comments     VARCHAR,
    glusr_premium_updatedby_url     VARCHAR,
    category_type                   VARCHAR,
    location_type                   VARCHAR,
    location_iso                    VARCHAR,
    category_location_credit_value  DOUBLE
)
WITH (
    -- Replace <GCS_BUCKET> and <GCS_GLUSR_PREMIUM_LISTING_PREFIX> before running
    external_location        = 's3://<GCS_BUCKET>/<GCS_GLUSR_PREMIUM_LISTING_PREFIX>',
    format                   = 'TEXTFILE',
    textfile_field_separator = ',',
    skip_header_line_count   = 1
);
