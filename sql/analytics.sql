CREATE OR REPLACE VIEW analytics.v_daily_demand AS
SELECT
    observed_at::date AS demand_date,
    COUNT(*) AS observed_blocks,
    ROUND(AVG(load_mw)::numeric, 2) AS average_load_mw,
    ROUND(MAX(load_mw)::numeric, 2) AS peak_load_mw,
    ROUND(MIN(load_mw)::numeric, 2) AS minimum_load_mw,
    ROUND(STDDEV_POP(load_mw)::numeric, 2) AS load_stddev_mw,
    ROUND(AVG(temp_c)::numeric, 2) AS average_temp_c,
    ROUND(AVG(relative_humidity)::numeric, 2) AS average_relative_humidity,
    -- For 15-minute data, a complete day has 96 blocks. Change this divisor to
    -- 24 if your configured source frequency is hourly.
    ROUND(LEAST(100.0, COUNT(*) * 100.0 / 96)::numeric, 2) AS completeness_pct
FROM analytics.fact_observations
GROUP BY observed_at::date;

CREATE OR REPLACE VIEW analytics.v_monthly_demand AS
SELECT
    DATE_TRUNC('month', observed_at)::date AS demand_month,
    COUNT(*) AS observed_blocks,
    ROUND(AVG(load_mw)::numeric, 2) AS average_load_mw,
    ROUND(MAX(load_mw)::numeric, 2) AS peak_load_mw,
    ROUND(MIN(load_mw)::numeric, 2) AS minimum_load_mw,
    ROUND(STDDEV_POP(load_mw)::numeric, 2) AS load_stddev_mw,
    ROUND(AVG(temp_c)::numeric, 2) AS average_temp_c,
    ROUND(AVG(relative_humidity)::numeric, 2) AS average_relative_humidity
FROM analytics.fact_observations
GROUP BY DATE_TRUNC('month', observed_at)::date;

CREATE OR REPLACE VIEW analytics.v_hourly_profile AS
SELECT
    EXTRACT(HOUR FROM observed_at)::integer AS hour_of_day,
    ROUND(AVG(load_mw)::numeric, 2) AS average_load_mw,
    ROUND(MAX(load_mw)::numeric, 2) AS peak_load_mw,
    COUNT(*) AS observation_count
FROM analytics.fact_observations
GROUP BY EXTRACT(HOUR FROM observed_at)::integer;

CREATE OR REPLACE VIEW analytics.v_month_hour_profile AS
SELECT
    EXTRACT(MONTH FROM observed_at)::integer AS month_number,
    EXTRACT(HOUR FROM observed_at)::integer AS hour_of_day,
    ROUND(AVG(load_mw)::numeric, 2) AS average_load_mw,
    COUNT(*) AS observation_count
FROM analytics.fact_observations
GROUP BY
    EXTRACT(MONTH FROM observed_at)::integer,
    EXTRACT(HOUR FROM observed_at)::integer;

-- One row per pipeline execution. Use this view for run-level KPI cards and history.
CREATE OR REPLACE VIEW analytics.v_pipeline_run_summary AS
SELECT
    run_id, source_file, started_at, completed_at, status,
    input_rows, staging_rows, eligible_rows,
    duplicate_timestamp_rows, missing_value_rows, invalid_range_rows,
    load_jump_rows, missing_interval_count, unexpected_interval_count,
    invalid_datetime_rows, issue_count, duration_seconds, error_message
FROM analytics.pipeline_runs;

-- Long format for visualising individual issue categories by pipeline run.
CREATE OR REPLACE VIEW analytics.v_issue_counts_by_run AS
SELECT
    p.run_id,
    p.started_at,
    p.status,
    i.issue_type,
    i.severity,
    COUNT(i.issue_id) AS issue_count
FROM analytics.pipeline_runs p
LEFT JOIN analytics.data_quality_issues i ON i.run_id = p.run_id
GROUP BY p.run_id, p.started_at, p.status, i.issue_type, i.severity;

CREATE OR REPLACE VIEW analytics.v_latest_run_issues AS
SELECT i.*
FROM analytics.data_quality_issues i
WHERE i.run_id = (
    SELECT run_id
    FROM analytics.pipeline_runs
    WHERE status = 'SUCCESS'
    ORDER BY completed_at DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW analytics.v_flagged_anomalies AS
SELECT
    issue_id,
    run_id,
    source_row_number,
    observed_at,
    issue_type,
    severity,
    details,
    created_at
FROM analytics.v_latest_run_issues
WHERE issue_type IN (
    'load_jump', 'duplicate_timestamp', 'invalid_load', 'invalid_temp',
    'invalid_humidity', 'unexpected_interval', 'invalid_datetime'
);
