-- tests/integration/sql/mysql_test_patch.sql
-- -----------------------------------------------------------------------------
-- Test-only adjustments applied after the real migrations, so the integration
-- stack matches the columns the CDC loader and the Spark job read/write.
-- Never mounted into the dev stack.
--   * stg_raw_orders.total_amount: read by transform_orders.py, written by CDC
--   * stg_raw_orders.updated_at:   CDC watermark column, written to staging
--   * source_order_id nullable:    lets the quarantine test insert a null key
-- -----------------------------------------------------------------------------

USE ecommerce_staging;

ALTER TABLE stg_raw_orders
    ADD COLUMN total_amount DECIMAL(12,2) NULL,
    ADD COLUMN updated_at   DATETIME NULL,
    MODIFY COLUMN source_order_id VARCHAR(100) NULL;
