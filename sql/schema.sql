CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.pipeline_runs (
    run_id UUID PRIMARY KEY,
    source_file TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'RUNNING',
    input_rows INTEGER NOT NULL DEFAULT 0,
    staging_rows INTEGER NOT NULL DEFAULT 0,
    eligible_rows INTEGER NOT NULL DEFAULT 0,
    duplicate_timestamp_rows INTEGER NOT NULL DEFAULT 0,
    missing_value_rows INTEGER NOT NULL DEFAULT 0,
    invalid_range_rows INTEGER NOT NULL DEFAULT 0,
    load_jump_rows INTEGER NOT NULL DEFAULT 0,
    missing_interval_count INTEGER NOT NULL DEFAULT 0,
    unexpected_interval_count INTEGER NOT NULL DEFAULT 0,
    invalid_datetime_rows INTEGER NOT NULL DEFAULT 0,
    issue_count INTEGER NOT NULL DEFAULT 0,
    duration_seconds NUMERIC(12,3),
    error_message TEXT
);

-- Staging is a snapshot of the most recently processed file. The source CSV is
-- left untouched in data/raw/; quality issue history is persisted separately.
CREATE TABLE IF NOT EXISTS analytics.staging_observations (
    run_id UUID NOT NULL,
    source_row_number INTEGER NOT NULL,
    raw_datetime TEXT,
    observed_at TIMESTAMP,
    load_mw DOUBLE PRECISION,
    temp_c DOUBLE PRECISION,
    relative_humidity DOUBLE PRECISION,
    is_duplicate_timestamp BOOLEAN NOT NULL DEFAULT FALSE,
    has_missing_value BOOLEAN NOT NULL DEFAULT FALSE,
    invalid_load BOOLEAN NOT NULL DEFAULT FALSE,
    invalid_temp BOOLEAN NOT NULL DEFAULT FALSE,
    invalid_humidity BOOLEAN NOT NULL DEFAULT FALSE,
    is_load_jump BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, source_row_number)
);

-- Curated table: one row per valid, unambiguous timestamp. Weather fields can
-- be NULL if just that weather field failed validation. Load jumps stay visible.
CREATE TABLE IF NOT EXISTS analytics.fact_observations (
    observed_at TIMESTAMP PRIMARY KEY,
    load_mw DOUBLE PRECISION NOT NULL,
    temp_c DOUBLE PRECISION,
    relative_humidity DOUBLE PRECISION,
    source_row_number INTEGER NOT NULL,
    is_load_jump BOOLEAN NOT NULL DEFAULT FALSE,
    last_updated_run UUID REFERENCES analytics.pipeline_runs(run_id),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.data_quality_issues (
    issue_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES analytics.pipeline_runs(run_id),
    source_row_number INTEGER,
    observed_at TIMESTAMP,
    issue_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    details TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_observations_date
    ON analytics.fact_observations (observed_at);
CREATE INDEX IF NOT EXISTS idx_quality_issues_run_type
    ON analytics.data_quality_issues (run_id, issue_type);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started
    ON analytics.pipeline_runs (started_at DESC);
