-- tests/integration/sql/postgres_analytics_test.sql
-- -----------------------------------------------------------------------------
-- Test-only copy of migrations/postgres/002_create_analytics_tables.sql.
-- The only change: CREATE INDEX -> CREATE INDEX IF NOT EXISTS. Several index
-- names are also created by 001_create_warehouse_schema.sql, and the official
-- postgres entrypoint runs init SQL with ON_ERROR_STOP=1, so the duplicate
-- statement aborts container init. Regenerate from the migration if it changes.
-- -----------------------------------------------------------------------------

-- =====================================================
-- ANALYTICS TABLES FOR CLICKSTREAM AND AGGREGATES
-- =====================================================

-- Clickstream fact table
CREATE TABLE IF NOT EXISTS warehouse.fact_clickstream (
    event_id        VARCHAR(100) PRIMARY KEY,
    user_id         BIGINT,
    product_id      BIGINT,
    date_id         INTEGER,
    session_id      VARCHAR(255) NOT NULL,
    event_type      VARCHAR(50) NOT NULL,
    page            VARCHAR(500),
    device_type     VARCHAR(50),
    browser         VARCHAR(50),
    os              VARCHAR(50),
    session_duration_seconds INTEGER,
    is_bot          BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bot traffic table
CREATE TABLE IF NOT EXISTS warehouse.bot_traffic (
    event_id        VARCHAR(100) PRIMARY KEY,
    session_id      VARCHAR(255) NOT NULL,
    user_id         BIGINT,
    event_count     INTEGER,
    first_event_at  TIMESTAMP,
    last_event_at   TIMESTAMP,
    duration_seconds INTEGER,
    reason          VARCHAR(100),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily Revenue Aggregates
CREATE TABLE IF NOT EXISTS warehouse.agg_daily_revenue (
    date_id         INTEGER NOT NULL,
    date            DATE NOT NULL,
    total_orders    INTEGER DEFAULT 0,
    total_revenue   DECIMAL(15,2) DEFAULT 0,
    avg_order_value DECIMAL(10,2) DEFAULT 0,
    total_discount_given DECIMAL(15,2) DEFAULT 0,
    unique_customers INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_id)
);

-- Product Performance Aggregates
CREATE TABLE IF NOT EXISTS warehouse.agg_product_performance (
    date_id         INTEGER NOT NULL,
    date            DATE NOT NULL,
    product_id      BIGINT NOT NULL,
    units_sold      INTEGER DEFAULT 0,
    revenue         DECIMAL(15,2) DEFAULT 0,
    returns         INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5,2) DEFAULT 0,
    product_clicks  INTEGER DEFAULT 0,
    product_views   INTEGER DEFAULT 0,
    add_to_carts    INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_id, product_id)
);

-- User Cohort Aggregates
CREATE TABLE IF NOT EXISTS warehouse.agg_user_cohorts (
    signup_month    VARCHAR(7) NOT NULL,  -- YYYY-MM
    cohort_month    VARCHAR(7) NOT NULL,  -- YYYY-MM
    active_users    INTEGER DEFAULT 0,
    revenue_per_user DECIMAL(10,2) DEFAULT 0,
    retention_rate  DECIMAL(5,2) DEFAULT 0,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (signup_month, cohort_month)
);

-- Inventory Health Aggregates
CREATE TABLE IF NOT EXISTS warehouse.agg_inventory_health (
    product_id      BIGINT NOT NULL,
    warehouse_id    VARCHAR(100) NOT NULL,
    date            DATE NOT NULL,
    quantity_on_hand INTEGER NOT NULL,
    avg_daily_sales DECIMAL(10,2) DEFAULT 0,
    days_of_stock_remaining INTEGER,
    reorder_flag    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, warehouse_id, date)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_fact_clickstream_session ON warehouse.fact_clickstream(session_id);
CREATE INDEX IF NOT EXISTS idx_fact_clickstream_date ON warehouse.fact_clickstream(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_clickstream_user ON warehouse.fact_clickstream(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_clickstream_product ON warehouse.fact_clickstream(product_id);

CREATE INDEX IF NOT EXISTS idx_agg_daily_revenue_date ON warehouse.agg_daily_revenue(date_id);
CREATE INDEX IF NOT EXISTS idx_agg_product_performance_date ON warehouse.agg_product_performance(date_id);
CREATE INDEX IF NOT EXISTS idx_agg_product_performance_product ON warehouse.agg_product_performance(product_id);