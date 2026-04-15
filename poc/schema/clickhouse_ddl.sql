-- =============================================================================
-- ClickHouse DDL — event_fact table
-- Engine   : MergeTree (ReplacingMergeTree variant commented for update tests)
-- Partition : PARTITION BY toYYYYMM(event_date)
-- Sort key  : ORDER BY (event_date, event_type, user_id)  — supports date-range
--             and aggregation queries efficiently with Z-order-like pruning
-- Compression: LZ4 (default); ZSTD(1) on cold columns commented below
-- =============================================================================

-- Drop and recreate for clean benchmark run
DROP TABLE IF EXISTS poc.event_fact;

CREATE TABLE poc.event_fact
(
    -- Primary / time dimensions
    event_id                Int64                                    COMMENT 'Unique row identifier',
    event_date              Date                                     COMMENT 'Partition key',
    event_ts                DateTime64(6, 'UTC')                     COMMENT 'Full timestamp (microsecond)',
    session_id              String                                   COMMENT 'UUID session identifier',
    user_id                 Int64                                    COMMENT 'User identifier',
    device_id               String                                   COMMENT 'Device fingerprint',

    -- Event classification
    event_type              LowCardinality(String)                   COMMENT 'click/view/purchase/scroll',
    event_subtype           LowCardinality(Nullable(String))         COMMENT 'Sub-classification',
    page_id                 Int32                                    COMMENT 'Page/screen identifier',
    page_name               Nullable(String)                         COMMENT 'Human-readable page name',
    referrer_url            Nullable(String)                         COMMENT 'HTTP referrer (nullable)',

    -- Marketing attribution
    campaign_id             Nullable(String)                         COMMENT 'Campaign tag (nullable)',
    campaign_channel        LowCardinality(Nullable(String))         COMMENT 'Channel attribution',
    ab_variant              LowCardinality(Nullable(String))         COMMENT 'A/B test bucket',

    -- Geo
    country_code            LowCardinality(Nullable(String))         COMMENT 'ISO 3166-1 alpha-2',
    region                  Nullable(String)                         COMMENT 'State/province',
    city                    Nullable(String)                         COMMENT 'City name',
    latitude                Nullable(Float64)                        COMMENT 'Geo latitude',
    longitude               Nullable(Float64)                        COMMENT 'Geo longitude',

    -- Client info
    ip_address              Nullable(String)                         COMMENT 'IPv4 or IPv6',
    user_agent              Nullable(String)                         COMMENT 'Raw browser UA string',
    os_family               LowCardinality(Nullable(String))         COMMENT 'OS family',
    browser_family          LowCardinality(Nullable(String))         COMMENT 'Browser family',
    device_type             LowCardinality(Nullable(String))         COMMENT 'desktop/mobile/tablet',
    screen_width            Nullable(Int16)                          COMMENT 'Screen width (px)',
    screen_height           Nullable(Int16)                          COMMENT 'Screen height (px)',
    viewport_width          Nullable(Int16)                          COMMENT 'Viewport width (px)',
    viewport_height         Nullable(Int16)                          COMMENT 'Viewport height (px)',

    -- Commerce
    product_id              Nullable(Int32)                          COMMENT 'Product SKU (nullable)',
    product_name            Nullable(String)                         COMMENT 'Product name (nullable)',
    product_category_l1     LowCardinality(Nullable(String))         COMMENT 'Top-level category',
    product_category_l2     LowCardinality(Nullable(String))         COMMENT 'Sub-category',
    product_price           Nullable(Decimal(12,2))                  COMMENT 'Unit price (nullable)',
    quantity                Nullable(Int16)                          COMMENT 'Items (nullable)',
    order_id                Nullable(Int64)                          COMMENT 'Order ID (nullable)',
    order_total             Nullable(Decimal(14,2))                  COMMENT 'Order total (nullable)',
    discount_amount         Nullable(Decimal(10,2))                  COMMENT 'Discount (nullable)',
    coupon_code             Nullable(String)                         COMMENT 'Coupon code (nullable)',
    revenue                 Nullable(Decimal(14,4))                  COMMENT 'Attributed revenue',

    -- Engagement
    duration_ms             Nullable(Int32)                          COMMENT 'Time spent (ms)',
    scroll_depth_pct        Nullable(Int8)                           COMMENT 'Scroll depth 0–100',
    click_x                 Nullable(Int16)                          COMMENT 'Click X coordinate',
    click_y                 Nullable(Int16)                          COMMENT 'Click Y coordinate',

    -- Flags
    is_bot                  Bool                                     COMMENT 'Bot detection flag',
    is_authenticated        Bool                                     COMMENT 'User logged in',
    is_first_visit          Bool                                     COMMENT 'First session for user',

    -- Experiment & infra
    experiment_id           Nullable(Int32)                          COMMENT 'Active experiment (nullable)',
    server_id               Nullable(Int16)                          COMMENT 'Backend server ID',
    load_time_ms            Nullable(Int32)                          COMMENT 'Page load time (ms)',
    ttfb_ms                 Nullable(Int16)                          COMMENT 'Time-to-first-byte (ms)',
    error_code              Nullable(Int16)                          COMMENT 'Error code (nullable)',
    error_message           Nullable(String)                         COMMENT 'Error description (nullable)',
    tag_list                Nullable(String)                         COMMENT 'Comma-separated tags',

    -- Semi-structured (ClickHouse JSON object type — 24.x experimental, use String fallback)
    custom_dimensions       String                                   COMMENT 'JSON key-value pairs',

    -- Pipeline metadata
    raw_payload_size_bytes  Nullable(Int32)                          COMMENT 'Event payload size',
    ingestion_ts            DateTime64(3, 'UTC')                     COMMENT 'Ingestion timestamp',
    processing_lag_ms       Nullable(Int32)                          COMMENT 'Ingestion - event_ts (ms)',
    data_version            Int8                                     COMMENT 'Schema version (1–5)',
    partition_key           Int32                                    COMMENT 'Synthetic partition hash',
    checksum                Nullable(Int64)                          COMMENT 'CRC64 integrity'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, user_id)
-- Primary key is a prefix of the ORDER BY — used for sparse index
PRIMARY KEY (event_date, event_type)
SETTINGS
    index_granularity         = 8192,
    min_bytes_for_wide_part   = 10485760,  -- 10 MB threshold for Wide vs Compact parts
    min_rows_for_wide_part    = 512000;

-- =============================================================================
-- Column-level CODEC overrides for high-cardinality / cold columns
-- Apply these after CREATE TABLE if desired:
-- ALTER TABLE poc.event_fact MODIFY COLUMN user_agent CODEC(ZSTD(1));
-- ALTER TABLE poc.event_fact MODIFY COLUMN referrer_url CODEC(ZSTD(1));
-- ALTER TABLE poc.event_fact MODIFY COLUMN ip_address CODEC(ZSTD(1));
-- =============================================================================

-- =============================================================================
-- ReplacingMergeTree variant — for W3/W4 update workload benchmarks
-- Comment out the MergeTree above and use this for mutation-less update testing
-- =============================================================================
/*
CREATE TABLE poc.event_fact_replacing
( ... same columns ... )
ENGINE = ReplacingMergeTree(ingestion_ts)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_id)       -- PK must uniquely identify rows for replacing semantics
SETTINGS index_granularity = 8192;
*/

-- Loading from local Parquet files:
-- INSERT INTO poc.event_fact
-- SELECT * FROM file('/opt1/data/event_fact/**/*.parquet', Parquet);

-- Loading from GCS via s3() table function:
-- INSERT INTO poc.event_fact
-- SELECT * FROM s3(
--     'https://storage.googleapis.com/<bucket>/olap_poc/data/event_fact/**/*.parquet',
--     '<HMAC_ACCESS_KEY>', '<HMAC_SECRET>',
--     'Parquet'
-- );

-- Verify (run manually or via make schema-clickhouse which issues these separately):
-- SELECT count() FROM poc.event_fact;
-- SHOW CREATE TABLE poc.event_fact;
