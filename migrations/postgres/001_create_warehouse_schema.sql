-- =====================================================
-- ECOMMERCE DATA PLATFORM - POSTGRESQL DATA WAREHOUSE
-- Star Schema Design
-- =====================================================

-- =====================================================
-- STEP 1: Create the warehouse schema
-- =====================================================
CREATE SCHEMA IF NOT EXISTS warehouse;

-- =====================================================
-- STEP 2: Create migration tracking table
-- =====================================================
CREATE TABLE IF NOT EXISTS warehouse.schema_migrations (
    version_number   INTEGER PRIMARY KEY,     -- Unique migration version (001, 002, 003)
    description      TEXT NOT NULL,           -- Human-readable description of change
    applied_by       VARCHAR(100) NOT NULL,   -- Username who applied the migration
    applied_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When migration was run
    checksum         VARCHAR(64) NOT NULL,    -- SHA-256 hash of migration file
    execution_time   INTEGER,                 -- How long migration took (milliseconds)
    is_successful    BOOLEAN DEFAULT TRUE    -- Whether migration succeeded
);

COMMENT ON SCHEMA warehouse IS 'Star schema data warehouse for e-commerce analytics';
COMMENT ON TABLE warehouse.schema_migrations IS 'Tracks all schema changes applied to the database';

-- =====================================================
-- STEP 3: DIMENSION TABLES (dim_*)
-- These store descriptive attributes that change slowly
-- =====================================================

-- -----------------------------------------------------
-- 3.1 dim_users - Customer information
-- -----------------------------------------------------
CREATE TABLE warehouse.dim_users (
    user_id         BIGINT PRIMARY KEY,        -- Unique customer identifier
    name            VARCHAR(255),              -- Customer's full name
    email           VARCHAR(255) NOT NULL,     -- Customer's email (used for login)
    city            VARCHAR(100),              -- City of residence
    country         VARCHAR(100),              -- Country of residence
    segment         VARCHAR(50),               -- Customer segment: 'Gold', 'Silver', 'Bronze', 'New'
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When user account was created
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- Last update to this row
);

COMMENT ON TABLE warehouse.dim_users IS 'Contains all customer/ user information';
COMMENT ON COLUMN warehouse.dim_users.user_id IS 'Surrogate key - unique identifier for each user';
COMMENT ON COLUMN warehouse.dim_users.segment IS 'Customer segment based on purchase history: Gold (high value), Silver (regular), Bronze (occasional), New (first 30 days)';

-- Create index for frequently queried columns
CREATE INDEX idx_dim_users_email ON warehouse.dim_users(email);
CREATE INDEX idx_dim_users_segment ON warehouse.dim_users(segment);

-- -----------------------------------------------------
-- 3.2 dim_products - Product catalog
-- -----------------------------------------------------
CREATE TABLE warehouse.dim_products (
    product_id      BIGINT PRIMARY KEY,        -- Unique product identifier
    name            VARCHAR(255) NOT NULL,     -- Product display name
    category        VARCHAR(100),              -- Top-level category (e.g., 'Electronics')
    subcategory     VARCHAR(100),              -- Second-level category (e.g., 'Laptops')
    brand           VARCHAR(100),              -- Product brand/manufacturer
    cost_price      DECIMAL(10,2),             -- What the company pays to acquire
    list_price      DECIMAL(10,2),             -- Original price before discounts
    is_active       BOOLEAN DEFAULT TRUE,      -- Whether product is still sold
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE warehouse.dim_products IS 'Product catalog dimension table';
COMMENT ON COLUMN warehouse.dim_products.cost_price IS 'Wholesale cost paid to supplier';
COMMENT ON COLUMN warehouse.dim_products.list_price IS 'Recommended retail price before any discounts';
COMMENT ON COLUMN warehouse.dim_products.is_active IS 'False for discontinued products';

CREATE INDEX idx_dim_products_category ON warehouse.dim_products(category);
CREATE INDEX idx_dim_products_brand ON warehouse.dim_products(brand);

-- -----------------------------------------------------
-- 3.3 dim_date - Time dimension for date-based analysis
-- -----------------------------------------------------
CREATE TABLE warehouse.dim_date (
    date_id         INTEGER PRIMARY KEY,       -- Format: YYYYMMDD (e.g., 20260115 for Jan 15, 2026)
    date            DATE NOT NULL,             -- Actual calendar date
    day             INTEGER,                   -- Day of month (1-31)
    month           INTEGER,                   -- Month number (1-12)
    quarter         INTEGER,                   -- Quarter number (1-4)
    year            INTEGER,                   -- Year (e.g., 2026)
    day_of_week     INTEGER,                   -- Day of week (1=Monday, 7=Sunday)
    day_name        VARCHAR(20),               -- Monday, Tuesday, etc.
    month_name      VARCHAR(20),               -- January, February, etc.
    is_weekend      BOOLEAN,                   -- TRUE if Saturday or Sunday
    is_holiday      BOOLEAN DEFAULT FALSE,     -- TRUE for national holidays
    holiday_name    VARCHAR(100)               -- Name of holiday (e.g., 'Christmas')
);

COMMENT ON TABLE warehouse.dim_date IS 'Date dimension for time-based aggregations - pre-populated with 10+ years of dates';
COMMENT ON COLUMN warehouse.dim_date.date_id IS 'Surrogate key in format YYYYMMDD for easy indexing';
COMMENT ON COLUMN warehouse.dim_date.is_weekend IS 'True for Saturday and Sunday';
COMMENT ON COLUMN warehouse.dim_date.is_holiday IS 'True for known holidays (can be updated annually)';

CREATE INDEX idx_dim_date_year_month ON warehouse.dim_date(year, month);
CREATE INDEX idx_dim_date_weekend ON warehouse.dim_date(is_weekend);

-- -----------------------------------------------------
-- 3.4 dim_location - Geographic dimension
-- -----------------------------------------------------
CREATE TABLE warehouse.dim_location (
    location_id     BIGINT PRIMARY KEY,        -- Surrogate key for location
    city            VARCHAR(100),              -- City name
    state           VARCHAR(100),              -- State/Province name
    country         VARCHAR(100) NOT NULL,     -- Country name
    pincode         VARCHAR(20),               -- Postal/ZIP code
    region          VARCHAR(50),               -- Sales region: 'North', 'South', 'East', 'West', 'International'
    timezone        VARCHAR(50)                -- IANA timezone (e.g., 'America/New_York')
);

COMMENT ON TABLE warehouse.dim_location IS 'Geographic location dimension for regional analysis';
COMMENT ON COLUMN warehouse.dim_location.region IS 'Sales territory assignment for regional reporting';

CREATE INDEX idx_dim_location_country_city ON warehouse.dim_location(country, city);
CREATE INDEX idx_dim_location_region ON warehouse.dim_location(region);

-- =====================================================
-- STEP 4: FACT TABLES (fact_*)
-- These store business events/transactions
-- =====================================================

-- -----------------------------------------------------
-- 4.1 fact_orders - Each row = one order line item
-- -----------------------------------------------------
CREATE TABLE warehouse.fact_orders (
    order_id        BIGINT NOT NULL,           -- Order identifier (can have multiple products)
    line_item_id    BIGINT NOT NULL,           -- Unique per product in an order
    user_id         BIGINT NOT NULL,           -- Foreign key to dim_users
    product_id      BIGINT NOT NULL,           -- Foreign key to dim_products
    date_id         INTEGER NOT NULL,          -- Foreign key to dim_date (order date)
    location_id     BIGINT NOT NULL,           -- Foreign key to dim_location (shipping address)
    quantity        INTEGER NOT NULL,          -- Number of units ordered
    unit_price      DECIMAL(10,2) NOT NULL,    -- Price per unit at time of order
    discount        DECIMAL(5,2) DEFAULT 0,    -- Discount percentage applied (0-100)
    total_amount    DECIMAL(12,2) NOT NULL,    -- Calculated: quantity * unit_price * (1 - discount/100)
    status          VARCHAR(50) DEFAULT 'pending',  -- pending, paid, shipped, delivered, cancelled, refunded
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE warehouse.fact_orders IS 'Order line items - one row per product in each order';
COMMENT ON COLUMN warehouse.fact_orders.order_id IS 'Order ID from source system (not unique in this table - one order can have multiple products)';
COMMENT ON COLUMN warehouse.fact_orders.line_item_id IS 'Unique identifier for each product in an order (composite key with order_id)';
COMMENT ON COLUMN warehouse.fact_orders.total_amount IS 'Final amount after discount = quantity * unit_price * (1 - discount/100)';

-- Composite primary key
ALTER TABLE warehouse.fact_orders ADD PRIMARY KEY (order_id, line_item_id);

-- Foreign keys
ALTER TABLE warehouse.fact_orders ADD CONSTRAINT fk_fact_orders_user FOREIGN KEY (user_id) REFERENCES warehouse.dim_users(user_id);
ALTER TABLE warehouse.fact_orders ADD CONSTRAINT fk_fact_orders_product FOREIGN KEY (product_id) REFERENCES warehouse.dim_products(product_id);
ALTER TABLE warehouse.fact_orders ADD CONSTRAINT fk_fact_orders_date FOREIGN KEY (date_id) REFERENCES warehouse.dim_date(date_id);
ALTER TABLE warehouse.fact_orders ADD CONSTRAINT fk_fact_orders_location FOREIGN KEY (location_id) REFERENCES warehouse.dim_location(location_id);

-- Indexes for query performance
CREATE INDEX idx_fact_orders_date ON warehouse.fact_orders(date_id);
CREATE INDEX idx_fact_orders_user ON warehouse.fact_orders(user_id);
CREATE INDEX idx_fact_orders_product ON warehouse.fact_orders(product_id);
CREATE INDEX idx_fact_orders_status ON warehouse.fact_orders(status);

-- -----------------------------------------------------
-- 4.2 fact_clickstream - Each row = one user action
-- -----------------------------------------------------
CREATE TABLE warehouse.fact_clickstream (
    event_id        BIGSERIAL PRIMARY KEY,    -- Auto-incrementing unique identifier
    user_id         BIGINT NOT NULL,          -- Foreign key to dim_users (can be NULL for anonymous)
    product_id      BIGINT,                   -- Foreign key to dim_products (NULL if not product page)
    date_id         INTEGER NOT NULL,          -- Foreign key to dim_date (event date)
    session_id      VARCHAR(255) NOT NULL,    -- Browser session identifier
    event_type      VARCHAR(50) NOT NULL,     -- page_view, add_to_cart, remove_from_cart, checkout_start, purchase
    page            VARCHAR(500),              -- URL path of the page
    referrer        VARCHAR(500),              -- Previous page URL
    device          VARCHAR(50),               -- mobile, tablet, desktop
    browser         VARCHAR(50),               -- Chrome, Firefox, Safari, etc.
    duration_seconds INTEGER,                  -- Time spent on page (seconds)
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE warehouse.fact_clickstream IS 'User click/event data for behavioral analysis and funnels';
COMMENT ON COLUMN warehouse.fact_clickstream.session_id IS 'Browser session ID - multiple events share same session';
COMMENT ON COLUMN warehouse.fact_clickstream.event_type IS 'Type of user interaction: page_view, add_to_cart, checkout, purchase';

-- Foreign keys
ALTER TABLE warehouse.fact_clickstream ADD CONSTRAINT fk_fact_clickstream_user FOREIGN KEY (user_id) REFERENCES warehouse.dim_users(user_id);
ALTER TABLE warehouse.fact_clickstream ADD CONSTRAINT fk_fact_clickstream_product FOREIGN KEY (product_id) REFERENCES warehouse.dim_products(product_id);
ALTER TABLE warehouse.fact_clickstream ADD CONSTRAINT fk_fact_clickstream_date FOREIGN KEY (date_id) REFERENCES warehouse.dim_date(date_id);

-- Indexes for query performance
CREATE INDEX idx_fact_clickstream_date ON warehouse.fact_clickstream(date_id);
CREATE INDEX idx_fact_clickstream_user ON warehouse.fact_clickstream(user_id);
CREATE INDEX idx_fact_clickstream_session ON warehouse.fact_clickstream(session_id);
CREATE INDEX idx_fact_clickstream_event_type ON warehouse.fact_clickstream(event_type);
CREATE INDEX idx_fact_clickstream_user_date ON warehouse.fact_clickstream(user_id, date_id);

-- =====================================================
-- STEP 5: Verification queries (run after creation)
-- =====================================================

-- List all tables in warehouse schema
-- SELECT table_name FROM information_schema.tables WHERE table_schema = 'warehouse';

-- Check that all tables were created
-- \dt warehouse.*