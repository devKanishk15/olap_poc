-- =============================================================================
-- Apache Doris DDL — event_fact table
-- Engine: Duplicate Key (for read-heavy OLAP workload)
--         Unique Key variant included as comment for update workloads.
-- Partition: RANGE by event_date (monthly buckets)
-- Distribution: HASH(user_id) BUCKETS 8  (fits 8 GB single-node)
-- =============================================================================

-- Create database first (idempotent)
CREATE DATABASE IF NOT EXISTS poc;
USE poc;

-- Drop if re-running
DROP TABLE IF EXISTS event_fact;

CREATE TABLE event_fact (
    -- Primary / time dimensions
    event_id                BIGINT          NOT NULL    COMMENT 'Unique row identifier',
    event_date              DATE            NOT NULL    COMMENT 'Partition key — date of event',
    event_ts                DATETIME(6)     NOT NULL    COMMENT 'Full event timestamp (microsecond)',
    session_id              VARCHAR(36)     NOT NULL    COMMENT 'UUID session identifier (high cardinality)',
    user_id                 BIGINT          NOT NULL    COMMENT 'User identifier (~500k distinct)',
    device_id               VARCHAR(40)     NOT NULL    COMMENT 'Device fingerprint (high cardinality)',

    -- Event classification
    event_type              VARCHAR(30)     NOT NULL    COMMENT 'click/view/purchase/scroll (low card)',
    event_subtype           VARCHAR(50)                 COMMENT 'Sub-classification (~200 distinct)',
    page_id                 INT             NOT NULL    COMMENT 'Page/screen identifier (~5k distinct)',
    page_name               VARCHAR(100)                COMMENT 'Human-readable page name',
    referrer_url            VARCHAR(500)                COMMENT 'HTTP referrer (nullable, high card)',

    -- Marketing attribution
    campaign_id             VARCHAR(50)                 COMMENT 'Campaign tag (~1k distinct, nullable)',
    campaign_channel        VARCHAR(30)                 COMMENT 'email/paid_search/organic/direct',
    ab_variant              VARCHAR(10)                 COMMENT 'A/B test bucket (A/B/C/control)',

    -- Geo
    country_code            CHAR(2)                     COMMENT 'ISO 3166-1 alpha-2 (~60 distinct)',
    region                  VARCHAR(50)                 COMMENT 'State/province (medium card)',
    city                    VARCHAR(80)                 COMMENT 'City name (high cardinality)',
    latitude                DOUBLE                      COMMENT 'Geo latitude',
    longitude               DOUBLE                      COMMENT 'Geo longitude',

    -- Client info
    ip_address              VARCHAR(45)                 COMMENT 'IPv4 or IPv6',
    user_agent              VARCHAR(300)                COMMENT 'Raw browser UA string',
    os_family               VARCHAR(30)                 COMMENT 'Windows/macOS/iOS/Android/Linux',
    browser_family          VARCHAR(30)                 COMMENT 'Chrome/Safari/Firefox/Edge',
    device_type             VARCHAR(20)                 COMMENT 'desktop/mobile/tablet',
    screen_width            SMALLINT                    COMMENT 'Screen resolution width (px)',
    screen_height           SMALLINT                    COMMENT 'Screen resolution height (px)',
    viewport_width          SMALLINT                    COMMENT 'Browser viewport width (px)',
    viewport_height         SMALLINT                    COMMENT 'Browser viewport height (px)',

    -- Commerce
    product_id              INT                         COMMENT 'Product SKU (~50k distinct, nullable)',
    product_name            VARCHAR(150)                COMMENT 'Product display name (nullable)',
    product_category_l1     VARCHAR(60)                 COMMENT 'Top-level category (~30 distinct)',
    product_category_l2     VARCHAR(80)                 COMMENT 'Sub-category (~300 distinct)',
    product_price           DECIMAL(12,2)               COMMENT 'Product unit price (nullable)',
    quantity                SMALLINT                    COMMENT 'Items added/purchased (nullable)',
    order_id                BIGINT                      COMMENT 'Order ID (nullable, purchase events only)',
    order_total             DECIMAL(14,2)               COMMENT 'Order total value (nullable)',
    discount_amount         DECIMAL(10,2)               COMMENT 'Discount applied (nullable)',
    coupon_code             VARCHAR(30)                 COMMENT 'Coupon code used (nullable)',
    revenue                 DECIMAL(14,4)               COMMENT 'Attributed revenue',

    -- Engagement metrics
    duration_ms             INT                         COMMENT 'Time spent in ms',
    scroll_depth_pct        TINYINT                     COMMENT 'Scroll depth 0–100',
    click_x                 SMALLINT                    COMMENT 'Click X coordinate',
    click_y                 SMALLINT                    COMMENT 'Click Y coordinate',

    -- Flags
    is_bot                  BOOLEAN         NOT NULL    COMMENT 'Bot detection flag',
    is_authenticated        BOOLEAN         NOT NULL    COMMENT 'User authenticated at event time',
    is_first_visit          BOOLEAN         NOT NULL    COMMENT 'First session for this user',

    -- Experiment & infra
    experiment_id           INT                         COMMENT 'Active experiment ID (nullable)',
    server_id               SMALLINT                    COMMENT 'Backend server ID',
    load_time_ms            INT                         COMMENT 'Page load time at event (ms)',
    ttfb_ms                 SMALLINT                    COMMENT 'Time-to-first-byte (ms)',
    error_code              SMALLINT                    COMMENT 'HTTP/app error code (nullable)',
    error_message           VARCHAR(255)                COMMENT 'Error description (nullable)',
    tag_list                VARCHAR(500)                COMMENT 'Comma-separated content tags',

    -- Semi-structured
    custom_dimensions       JSON                        COMMENT 'Key-value pairs for custom properties',

    -- Pipeline metadata
    raw_payload_size_bytes  INT                         COMMENT 'Original event payload size',
    ingestion_ts            DATETIME(3)     NOT NULL    COMMENT 'Pipeline ingestion timestamp',
    processing_lag_ms       INT                         COMMENT 'Ingestion minus event_ts (ms)',
    data_version            TINYINT         NOT NULL    COMMENT 'Schema version tag (1–5)',
    partition_key           INT             NOT NULL    COMMENT 'Synthetic partition hash (0–29)',
    checksum                BIGINT                      COMMENT 'CRC64 integrity check'
)
-- Duplicate Key: all columns are stored; good for analytical/read workloads
DUPLICATE KEY(event_id, event_date, user_id)
-- Range partition by event_date — one partition per calendar month
PARTITION BY RANGE(event_date) (
    PARTITION p2024_01 VALUES LESS THAN ('2024-02-01'),
    PARTITION p2024_02 VALUES LESS THAN ('2024-03-01'),
    PARTITION p2024_03 VALUES LESS THAN ('2024-04-01'),
    PARTITION p2024_04 VALUES LESS THAN ('2024-05-01'),
    PARTITION p2024_05 VALUES LESS THAN ('2024-06-01'),
    PARTITION p2024_06 VALUES LESS THAN ('2024-07-01'),
    PARTITION p2024_07 VALUES LESS THAN ('2024-08-01'),
    PARTITION p2024_08 VALUES LESS THAN ('2024-09-01'),
    PARTITION p2024_09 VALUES LESS THAN ('2024-10-01'),
    PARTITION p2024_10 VALUES LESS THAN ('2024-11-01'),
    PARTITION p2024_11 VALUES LESS THAN ('2024-12-01'),
    PARTITION p2024_12 VALUES LESS THAN ('2025-01-01'),
    PARTITION p2025_01 VALUES LESS THAN ('2025-02-01'),
    PARTITION p_future  VALUES LESS THAN (MAXVALUE)
)
-- Hash distribution on user_id — 8 buckets suitable for single-node
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_num"  = "1",             -- single-node: no replication
    "storage_format"   = "V2",            -- Segment V2 (columnar)
    "compression"      = "LZ4",           -- LZ4: good balance of speed/ratio
    "enable_unique_key_merge_on_write" = "false"
);

-- -----------------------------------------------------------------------------
-- Unique Key variant (for W3/W4 update workload benchmarks)
-- Uncomment and rename to event_fact_mow when testing Merge-on-Write semantics
-- -----------------------------------------------------------------------------
/*
CREATE TABLE event_fact_mow (
    event_id                BIGINT          NOT NULL,
    event_date              DATE            NOT NULL,
    event_ts                DATETIME(6)     NOT NULL,
    -- ... (same columns as above) ...
    checksum                BIGINT
)
UNIQUE KEY(event_id, event_date)
PARTITION BY RANGE(event_date) ( ... same partitions ... )
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_num"                 = "1",
    "compression"                     = "LZ4",
    "enable_unique_key_merge_on_write" = "true"  -- MoW for true UPDATE semantics
);
*/

-- Verify
SHOW CREATE TABLE event_fact\G
