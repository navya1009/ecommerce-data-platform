-- =====================================================
-- FIX: watermark precision
-- watermarks.last_extracted_at was TIMESTAMP (whole seconds) while the source
-- updated_at carries microseconds. The incremental CDC filter then re-read the
-- boundary batch on every run. DATETIME(6) keeps microseconds.
-- Fresh databases get this from 002_create_control_tables.sql; this migration
-- upgrades databases created before that change. Safe to re-run.
-- =====================================================

USE ecommerce_staging;

ALTER TABLE watermarks MODIFY last_extracted_at DATETIME(6) NOT NULL;
