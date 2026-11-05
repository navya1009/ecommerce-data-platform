-- =====================================================
-- ECOMMERCE DATA PLATFORM - MYSQL STAGING DATABASE
-- Using ecommerce_staging database (already created by docker-compose)
-- =====================================================

-- Step 1: Use the existing database
USE ecommerce_staging;

-- Step 2: Create migration tracking table
CREATE TABLE IF NOT EXISTS schema_migrations (
    version_number   INT PRIMARY KEY,
    description      TEXT NOT NULL,
    applied_by       VARCHAR(100) NOT NULL,
    applied_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    checksum         VARCHAR(64) NOT NULL,
    execution_time   INT,
    is_successful    BOOLEAN DEFAULT TRUE
);

-- Step 3: stg_raw_orders
CREATE TABLE IF NOT EXISTS stg_raw_orders (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_order_id VARCHAR(100) NOT NULL,
    source_line_id  VARCHAR(100),
    user_id         VARCHAR(100),
    product_id      VARCHAR(100),
    quantity        INT,
    unit_price      DECIMAL(10,2),
    discount        DECIMAL(5,2),
    order_status    VARCHAR(50),
    order_date      DATETIME,
    raw_data        JSON,
    ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id        VARCHAR(100),
    is_processed    BOOLEAN DEFAULT FALSE,
    processed_at    TIMESTAMP NULL,
    error_message   TEXT
);

-- Step 4: stg_raw_products
CREATE TABLE IF NOT EXISTS stg_raw_products (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_id       VARCHAR(100) NOT NULL,
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

-- Step 5: stg_raw_inventory
CREATE TABLE IF NOT EXISTS stg_raw_inventory (
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    product_id       VARCHAR(100) NOT NULL,
    warehouse_id     VARCHAR(100) NOT NULL,
    warehouse_name   VARCHAR(200),
    quantity_on_hand INT NOT NULL,
    reorder_level    INT,
    last_updated     DATETIME,
    raw_data         JSON,
    ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id         VARCHAR(100),
    is_processed     BOOLEAN DEFAULT FALSE,
    processed_at     TIMESTAMP NULL,
    error_message    TEXT
);

-- Step 6: stg_raw_clickstream
CREATE TABLE IF NOT EXISTS stg_raw_clickstream (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id        VARCHAR(100),
    user_id         VARCHAR(100),
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

-- Step 7: batch_metadata
CREATE TABLE IF NOT EXISTS batch_metadata (
    batch_id        VARCHAR(100) PRIMARY KEY,
    source_system   VARCHAR(100) NOT NULL,
    start_time      TIMESTAMP NOT NULL,
    end_time        TIMESTAMP,
    record_count    INT,
    error_count     INT DEFAULT 0,
    status          VARCHAR(50) DEFAULT 'running',
    created_by      VARCHAR(100)
);

-- Create indexes
CREATE INDEX idx_stg_orders_processed ON stg_raw_orders(is_processed, batch_id);
CREATE INDEX idx_stg_orders_source_id ON stg_raw_orders(source_order_id);
CREATE INDEX idx_stg_products_processed ON stg_raw_products(is_processed);
CREATE INDEX idx_stg_inventory_product ON stg_raw_inventory(product_id);
CREATE INDEX idx_stg_clickstream_processed ON stg_raw_clickstream(is_processed);
CREATE INDEX idx_stg_clickstream_session ON stg_raw_clickstream(session_id);
CREATE INDEX idx_batch_metadata_status ON batch_metadata(status);

-- Show completion message
SELECT 'Staging schema created successfully!' AS Status;