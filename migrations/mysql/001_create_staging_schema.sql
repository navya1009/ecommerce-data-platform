-- =====================================================
-- ECOMMERCE DATA PLATFORM - MYSQL STAGING DATABASE
-- Raw data landing zone before transformation
-- =====================================================

-- =====================================================
-- STEP 1: Create the staging schema
-- =====================================================
CREATE SCHEMA IF NOT EXISTS staging;
USE staging;

-- =====================================================
-- STEP 2: Create migration tracking table
-- =====================================================
CREATE TABLE IF NOT EXISTS staging.schema_migrations (
    version_number   INT PRIMARY KEY,
    description      TEXT NOT NULL,
    applied_by       VARCHAR(100) NOT NULL,
    applied_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    checksum         VARCHAR(64) NOT NULL,
    execution_time   INT,
    is_successful    BOOLEAN DEFAULT TRUE
);

-- =====================================================
-- STEP 3: Staging tables (stg_*)
-- These mirror source system structure exactly
-- =====================================================

-- -----------------------------------------------------
-- 3.1 stg_raw_orders - Raw order data from source
-- -----------------------------------------------------
CREATE TABLE staging.stg_raw_orders (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_order_id VARCHAR(100) NOT NULL,      -- Order ID from source system
    source_line_id  VARCHAR(100),               -- Line item ID from source
    user_id         VARCHAR(100),               -- User ID as string from source
    product_id      VARCHAR(100),               -- Product ID as string from source
    quantity        INT,
    unit_price      DECIMAL(10,2),
    discount        DECIMAL(5,2),
    order_status    VARCHAR(50),
    order_date      DATETIME,
    raw_data        JSON,                        -- Complete source JSON for reprocessing
    ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When pipeline read this
    batch_id        VARCHAR(100),                -- Batch identifier for debugging
    is_processed    BOOLEAN DEFAULT FALSE,      -- Whether moved to warehouse
    processed_at    TIMESTAMP NULL,              -- When moved to warehouse
    error_message   TEXT                         -- If processing failed, why?
);

COMMENT ON TABLE staging.stg_raw_orders IS 'Raw order data directly from source API/database - exact source format';
CREATE INDEX idx_stg_orders_processed ON staging.stg_raw_orders(is_processed, batch_id);
CREATE INDEX idx_stg_orders_source_id ON staging.stg_raw_orders(source_order_id);
CREATE INDEX idx_stg_orders_ingested ON staging.stg_raw_orders(ingested_at);

-- -----------------------------------------------------
-- 3.2 stg_raw_products - Raw product catalog
-- -----------------------------------------------------
CREATE TABLE staging.stg_raw_products (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_id       VARCHAR(100) NOT NULL,      -- Product ID from source
    name            VARCHAR(500),
    category        VARCHAR(200),
    brand           VARCHAR(200),
    cost_price      DECIMAL(10,2),
    selling_price   DECIMAL(10,2),
    is_available    BOOLEAN,
    raw_data        JSON,
    ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id        VARCHAR(100),
    is_processed    BOOLEAN DEFAULT FALSE,
    processed_at    TIMESTAMP NULL,
    error_message   TEXT
);

COMMENT ON TABLE staging.stg_raw_products IS 'Raw product data from source catalog API';
CREATE INDEX idx_stg_products_processed ON staging.stg_raw_products(is_processed);
CREATE INDEX idx_stg_products_source_id ON staging.stg_raw_products(source_id);

-- -----------------------------------------------------
-- 3.3 stg_raw_inventory - Raw inventory levels
-- -----------------------------------------------------
CREATE TABLE staging.stg_raw_inventory (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    product_id      VARCHAR(100) NOT NULL,
    warehouse_id    VARCHAR(100) NOT NULL,
    warehouse_name  VARCHAR(200),
    quantity_on_hand INT NOT NULL,
    reorder_level   INT,
    last_updated    DATETIME,
    raw_data        JSON,
    ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id        VARCHAR(100),
    is_processed    BOOLEAN DEFAULT FALSE,
    processed_at    TIMESTAMP NULL,
    error_message   TEXT
);

COMMENT ON TABLE staging.stg_raw_inventory IS 'Raw inventory levels from warehouse management system';
CREATE INDEX idx_stg_inventory_product ON staging.stg_raw_inventory(product_id);
CREATE INDEX idx_stg_inventory_warehouse ON staging.stg_raw_inventory(warehouse_id);
CREATE INDEX idx_stg_inventory_processed ON staging.stg_raw_inventory(is_processed);

-- -----------------------------------------------------
-- 3.4 stg_raw_clickstream - Raw user click events
-- -----------------------------------------------------
CREATE TABLE staging.stg_raw_clickstream (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id        VARCHAR(100),               -- Source event ID
    user_id         VARCHAR(100),               -- Can be NULL for anonymous
    session_id      VARCHAR(255) NOT NULL,
    event_type      VARCHAR(50) NOT NULL,
    page_url        VARCHAR(1000),
    product_id      VARCHAR(100),
    device_type     VARCHAR(50),
    browser_name    VARCHAR(50),
    event_timestamp DATETIME NOT NULL,
    raw_data        JSON,
    ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id        VARCHAR(100),
    is_processed    BOOLEAN DEFAULT FALSE,
    processed_at    TIMESTAMP NULL,
    error_message   TEXT
);

COMMENT ON TABLE staging.stg_raw_clickstream IS 'Raw clickstream data from tracking pixel/analytics API';
CREATE INDEX idx_stg_clickstream_processed ON staging.stg_raw_clickstream(is_processed);
CREATE INDEX idx_stg_clickstream_session ON staging.stg_raw_clickstream(session_id);
CREATE INDEX idx_stg_clickstream_timestamp ON staging.stg_raw_clickstream(event_timestamp);

-- =====================================================
-- STEP 4: Staging metadata table
-- =====================================================
CREATE TABLE staging.batch_metadata (
    batch_id        VARCHAR(100) PRIMARY KEY,
    source_system   VARCHAR(100) NOT NULL,      -- 'shopify', 'bigcommerce', 'custom'
    start_time      TIMESTAMP NOT NULL,
    end_time        TIMESTAMP,
    record_count    INT,
    error_count     INT DEFAULT 0,
    status          VARCHAR(50) DEFAULT 'running',  -- running, completed, failed
    created_by      VARCHAR(100)
);

COMMENT ON TABLE staging.batch_metadata IS 'Tracks each batch of ingested data for monitoring';
CREATE INDEX idx_batch_metadata_status ON staging.batch_metadata(status);
CREATE INDEX idx_batch_metadata_source ON staging.batch_metadata(source_system);