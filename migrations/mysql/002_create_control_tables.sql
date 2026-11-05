-- =====================================================
-- PIPELINE CONTROL TABLES
-- Tracks runs, watermarks, and errors
-- =====================================================

USE ecommerce_staging;

-- Table 1: Pipeline Runs (audit log of every execution)
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          VARCHAR(100) PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    source_system   VARCHAR(100),
    batch_id        VARCHAR(100),
    start_time      TIMESTAMP NOT NULL,
    end_time        TIMESTAMP,
    status          VARCHAR(50) DEFAULT 'running',  -- running, success, failed
    rows_extracted  INT DEFAULT 0,
    rows_loaded     INT DEFAULT 0,
    error_count     INT DEFAULT 0,
    error_message   TEXT
);

-- Table 2: Watermarks (tracks last extracted timestamp)
CREATE TABLE IF NOT EXISTS watermarks (
    id              INT PRIMARY KEY AUTO_INCREMENT,
    pipeline_name   VARCHAR(100) NOT NULL UNIQUE,
    last_extracted_at DATETIME(6) NOT NULL,  -- microseconds: must match source updated_at precision
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    metadata        JSON
);