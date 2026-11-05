-- =====================================================
-- PIPELINE METRICS: one row per tracked pipeline run / task
-- Written by src/utils/metrics.py (track_metrics).
-- =====================================================

CREATE TABLE IF NOT EXISTS warehouse.pipeline_metrics (
    id                         BIGSERIAL PRIMARY KEY,
    pipeline_run_id            VARCHAR(100) NOT NULL,
    pipeline_name              VARCHAR(100) NOT NULL,
    task_name                  VARCHAR(100),
    status                     VARCHAR(20) NOT NULL DEFAULT 'success',
    rows_extracted             BIGINT DEFAULT 0,
    rows_transformed           BIGINT DEFAULT 0,
    rows_loaded                BIGINT DEFAULT 0,
    rows_quarantined           BIGINT DEFAULT 0,
    processing_time_seconds    NUMERIC(10,3),
    data_freshness_lag_minutes NUMERIC(10,2),
    recorded_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_metrics_run_id ON warehouse.pipeline_metrics (pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_metrics_name_time ON warehouse.pipeline_metrics (pipeline_name, recorded_at);
