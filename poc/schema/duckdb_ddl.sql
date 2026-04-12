-- =============================================================================
-- DuckDB DDL — event_fact table
-- DuckDB is an in-process engine; no explicit partitioning or sort keys in DDL.
-- Partitioning is handled at the file level (Parquet hive-partitioned by event_date).
-- =============================================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS poc;

-- Drop if re-running
DROP TABLE IF EXISTS poc.event_fact;

CREATE TABLE poc.event_fact (
    -- Primary / time dimensions
    event_id                BIGINT          NOT NULL,
    event_date              DATE            NOT NULL,
    event_ts                TIMESTAMPTZ     NOT NULL,       -- microsecond precision in DuckDB
    session_id              VARCHAR(36)     NOT NULL,
    user_id                 BIGINT          NOT NULL,
    device_id               VARCHAR(40)     NOT NULL,

    -- Event classification
    event_type              VARCHAR(30)     NOT NULL,
    event_subtype           VARCHAR(50),
    page_id                 INTEGER         NOT NULL,
    page_name               VARCHAR(100),
    referrer_url            VARCHAR(500),

    -- Marketing attribution
    campaign_id             VARCHAR(50),
    campaign_channel        VARCHAR(30),
    ab_variant              VARCHAR(10),

    -- Geo
    country_code            VARCHAR(2),
    region                  VARCHAR(50),
    city                    VARCHAR(80),
    latitude                DOUBLE,
    longitude               DOUBLE,

    -- Client info
    ip_address              VARCHAR(45),
    user_agent              VARCHAR(300),
    os_family               VARCHAR(30),
    browser_family          VARCHAR(30),
    device_type             VARCHAR(20),
    screen_width            SMALLINT,
    screen_height           SMALLINT,
    viewport_width          SMALLINT,
    viewport_height         SMALLINT,

    -- Commerce
    product_id              INTEGER,
    product_name            VARCHAR(150),
    product_category_l1     VARCHAR(60),
    product_category_l2     VARCHAR(80),
    product_price           DECIMAL(12,2),
    quantity                SMALLINT,
    order_id                BIGINT,
    order_total             DECIMAL(14,2),
    discount_amount         DECIMAL(10,2),
    coupon_code             VARCHAR(30),
    revenue                 DECIMAL(14,4),

    -- Engagement
    duration_ms             INTEGER,
    scroll_depth_pct        TINYINT,
    click_x                 SMALLINT,
    click_y                 SMALLINT,

    -- Flags
    is_bot                  BOOLEAN         NOT NULL,
    is_authenticated        BOOLEAN         NOT NULL,
    is_first_visit          BOOLEAN         NOT NULL,

    -- Experiment & infra
    experiment_id           INTEGER,
    server_id               SMALLINT,
    load_time_ms            INTEGER,
    ttfb_ms                 SMALLINT,
    error_code              SMALLINT,
    error_message           VARCHAR(255),
    tag_list                VARCHAR(500),

    -- Semi-structured (DuckDB native JSON type)
    custom_dimensions       JSON,

    -- Pipeline metadata
    raw_payload_size_bytes  INTEGER,
    ingestion_ts            TIMESTAMP       NOT NULL,
    processing_lag_ms       INTEGER,
    data_version            TINYINT         NOT NULL,
    partition_key           INTEGER         NOT NULL,
    checksum                BIGINT
);

-- =============================================================================
-- Loading from local Parquet (hive-partitioned)
-- Use this INSERT or run via the benchmark harness
-- =============================================================================
-- INSERT INTO poc.event_fact
-- SELECT * FROM read_parquet(
--     '/opt1/data/event_fact/**/*.parquet',
--     hive_partitioning = true
-- );

-- =============================================================================
-- Loading from GCS (via httpfs / S3-compatible API)
-- Requires: LOAD httpfs; SET s3_endpoint='storage.googleapis.com'; SET s3_access_key_id=...; SET s3_secret_access_key=...;
-- =============================================================================
-- INSERT INTO poc.event_fact
-- SELECT * FROM read_parquet(
--     's3://<GCS_BUCKET>/olap_poc/data/event_fact/**/*.parquet',
--     hive_partitioning = true
-- );

-- Verify schema
DESCRIBE poc.event_fact;
