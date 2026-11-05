-- =====================================================================
-- 009: agg_inventory_health.product_id is BIGINT, as migration 002 defines it
-- =====================================================================
-- The dev table was created with product_id as TEXT, so the inventory queries
-- (which join it to dim_products.product_id BIGINT) failed with
-- "operator does not exist: bigint = text". Every value was checked first:
-- all product_id values match ^[0-9]+$, so the cast cannot fail.
-- The guard checks the current type, so the migration is idempotent and does
-- nothing on a table that already has BIGINT (the test database).
-- =====================================================================

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'warehouse'
                 AND table_name = 'agg_inventory_health'
                 AND column_name = 'product_id'
                 AND data_type <> 'bigint') THEN
        ALTER TABLE warehouse.agg_inventory_health
            ALTER COLUMN product_id TYPE BIGINT USING product_id::bigint;
    END IF;
END $$;
