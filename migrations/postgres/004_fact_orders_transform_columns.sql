-- =====================================================
-- FIX: align warehouse.fact_orders with what transform_orders.py writes.
-- The Spark job inserts user_segment and gross_margin, which the original
-- schema lacks, and never sets location_id (NOT NULL in the schema), so
-- every load failed with "column user_segment does not exist".
-- Idempotent: safe to re-run on existing databases.
-- =====================================================

ALTER TABLE warehouse.fact_orders ADD COLUMN IF NOT EXISTS user_segment VARCHAR(50);
ALTER TABLE warehouse.fact_orders ADD COLUMN IF NOT EXISTS gross_margin DECIMAL(12,2);
ALTER TABLE warehouse.fact_orders ALTER COLUMN location_id DROP NOT NULL;
