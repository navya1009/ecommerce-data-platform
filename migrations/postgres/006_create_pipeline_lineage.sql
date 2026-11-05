-- =====================================================
-- PIPELINE LINEAGE: source -> target edges per run
-- Written by src/utils/lineage.py (record_lineage).
-- =====================================================

CREATE TABLE IF NOT EXISTS warehouse.pipeline_lineage (
    id                      BIGSERIAL PRIMARY KEY,
    run_id                  VARCHAR(100) NOT NULL,
    source_system           VARCHAR(100),
    source_table            VARCHAR(200),
    target_table            VARCHAR(200),
    rows_written            BIGINT DEFAULT 0,
    transformation_applied  VARCHAR(500),
    recorded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_lineage_run_id ON warehouse.pipeline_lineage (run_id);
CREATE INDEX IF NOT EXISTS idx_lineage_source_table ON warehouse.pipeline_lineage (source_table);
CREATE INDEX IF NOT EXISTS idx_lineage_target_table ON warehouse.pipeline_lineage (target_table);
