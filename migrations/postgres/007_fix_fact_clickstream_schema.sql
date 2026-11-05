-- =====================================================================
-- FIX: reconcile warehouse.fact_clickstream with transform_clickstream.py
--
-- Two different definitions of this table exist in the wild:
--   * 001_create_warehouse_schema.sql creates it with device, duration_seconds
--     and event_id BIGSERIAL, and no os / is_bot / session_duration_seconds.
--     This is what a fresh database gets, including the integration-test stack.
--   * 002_create_analytics_tables.sql defines it with device_type, os,
--     session_duration_seconds, is_bot and event_id VARCHAR. Its CREATE TABLE
--     IF NOT EXISTS is skipped when 001 has run first, so only the deployed
--     development database has this shape (it holds the loaded clickstream data).
-- The transform writes the 002 shape. This migration brings both databases to
-- one definition without dropping any column and without rewriting data.
-- Idempotent: safe to run on either shape.
-- =====================================================================

-- 1. device_type -> device (the analytics queries and the transform use "device")
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'warehouse' AND table_name = 'fact_clickstream'
                 AND column_name = 'device_type')
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_schema = 'warehouse' AND table_name = 'fact_clickstream'
                         AND column_name = 'device') THEN
        ALTER TABLE warehouse.fact_clickstream RENAME COLUMN device_type TO device;
    END IF;
END $$;

-- 2. Columns the transform writes that the 001 shape lacks
ALTER TABLE warehouse.fact_clickstream ADD COLUMN IF NOT EXISTS device VARCHAR(50);
ALTER TABLE warehouse.fact_clickstream ADD COLUMN IF NOT EXISTS os VARCHAR(50);
ALTER TABLE warehouse.fact_clickstream ADD COLUMN IF NOT EXISTS session_duration_seconds INTEGER;
ALTER TABLE warehouse.fact_clickstream ADD COLUMN IF NOT EXISTS is_bot BOOLEAN NOT NULL DEFAULT FALSE;

-- 3. event_id: the transform writes the source event id (for example EVT_000000696),
--    which is not numeric, so it cannot stay BIGSERIAL. Convert the 001 shape to
--    VARCHAR. The deployed shape is already VARCHAR and is left alone.
DO $$
BEGIN
    IF (SELECT data_type FROM information_schema.columns
        WHERE table_schema = 'warehouse' AND table_name = 'fact_clickstream'
          AND column_name = 'event_id') = 'bigint' THEN
        ALTER TABLE warehouse.fact_clickstream ALTER COLUMN event_id DROP DEFAULT;
        ALTER TABLE warehouse.fact_clickstream
            ALTER COLUMN event_id TYPE VARCHAR(100) USING event_id::text;
    END IF;
END $$;

-- 4. Documentation for the columns whose meaning matters downstream
COMMENT ON COLUMN warehouse.fact_clickstream.device IS
    'Device class: mobile, tablet or desktop. Was device_type in the deployed table.';
COMMENT ON COLUMN warehouse.fact_clickstream.is_bot IS
    'TRUE for events in sessions flagged as bot traffic (>200 events in under 60 seconds). Analytics exclude these.';
COMMENT ON COLUMN warehouse.fact_clickstream.created_at IS
    'Event time (event_timestamp from the source) for rows written by the current transform. Rows loaded by earlier versions hold load time.';
-- duration_seconds exists only in the 001 shape; the deployed 002 shape never had it,
-- so this comment is conditional. Without the guard, the migration fails on the deployed table.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'warehouse' AND table_name = 'fact_clickstream'
                 AND column_name = 'duration_seconds') THEN
        COMMENT ON COLUMN warehouse.fact_clickstream.duration_seconds IS
            'Deprecated: superseded by session_duration_seconds. Kept so existing queries do not break.';
    END IF;
END $$;
