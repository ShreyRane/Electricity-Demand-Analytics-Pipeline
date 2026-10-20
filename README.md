# Electricity Demand Analytics & Data Quality Pipeline

An end-to-end data engineering and analytics project built with **Python, Pandas, PostgreSQL, SQL, and Power BI**. The project processes electricity-load and weather observations, checks data quality, stores observations in PostgreSQL, and exposes analytical views for reporting.

## Project at a glance

- **Source data:** Tata Power electricity-load and weather CSV
- **Rows processed in the recorded run:** 234,272
- **Database:** PostgreSQL — `electricity_analytics`
- **Processing:** Python and Pandas
- **Analytics:** PostgreSQL SQL views
- **Visualization:** Power BI Desktop

> **Data-quality note:** The recorded pipeline run flagged 70,176 missing time intervals and 115 invalid-range rows. These results should be investigated and explained before treating the dataset as fully validated. The configured expected interval must match the source data's actual sampling frequency.

## Architecture

```text
Tata Power CSV
     |
     v
Python + Pandas
  - Parse timestamps and measurements
  - Check duplicates, missing values and ranges
  - Detect unexpected time gaps and load jumps
     |
     v
PostgreSQL
  - analytics.staging_observations
  - analytics.fact_observations
  - analytics.data_quality_issues
  - analytics.pipeline_runs
     |
     v
SQL analytics views
  - Daily and monthly demand
  - Hourly and month-hour patterns
  - Data-quality and pipeline summaries
     |
     v
Power BI report
  - Demand overview
  - Weather trends
  - Data-quality monitoring (planned enhancement)
```

## Repository structure

```text
electricity-demand-analytics-pipeline/
├── data/
│   ├── sample_load_weather.csv
│   └── Tata_Power(Sept).csv
├── outputs/
│   ├── run_summary_<run_id>.csv
│   └── quality_issues_<run_id>.csv
├── sql/
│   ├── schema.sql
│   └── analytics.sql
├── src/
│   └── pipeline.py
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

The exact local folder contents may vary. Keep the original source data unchanged where possible. Do not commit confidential data or credentials to GitHub.

## Technology stack

| Technology | Role |
|---|---|
| Python | Pipeline orchestration and data processing |
| Pandas | CSV ingestion, parsing, validation and transformation |
| PostgreSQL | Staging, curated observations, issue history and run audit |
| SQL | Reusable analytical views and database-level analysis |
| Power BI Desktop | Interactive charts and reporting |
| Git / GitHub | Version control and project documentation |

## Setup on Windows

### 1. Prerequisites

Install Python 3.11 or newer and PostgreSQL. Use pgAdmin or `psql` to create a database named:

```sql
CREATE DATABASE electricity_analytics;
```

If the database already exists, do not create it again.

### 2. Create and activate a virtual environment

Run these commands from the project root in PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

If `.env` already exists, do not overwrite it with the copy command.

### 3. Configure the database connection

Edit `.env` and configure the PostgreSQL connection settings for your local environment. Set the expected timestamp frequency to match the source data, for example:

```text
EXPECTED_FREQUENCY=15min
```

Use `15min` only if the data is expected every 15 minutes; use `1h` for hourly observations. Confirm the actual cadence before relying on missing-interval results.

Do not put your real `.env` file or password in GitHub. If a password contains special characters in a database URL, URL-encode them.

### 4. Create the database schema

In pgAdmin, select the `electricity_analytics` database, open Query Tool, and execute:

1. `sql/schema.sql`
2. Run the pipeline once.
3. Execute `sql/analytics.sql` to create the analytics views.

The SQL file path itself is not a SQL command. Open the file and execute its contents in Query Tool.

### 5. Run a sample test

```powershell
python src/pipeline.py data/sample_load_weather.csv
```

The included sample CSV is synthetic test data designed to exercise validation checks.

### 6. Run the Tata Power dataset

The local dataset used for this project is:

```text
data/Tata_Power(Sept).csv
```

Run it from the project root:

```powershell
python src/pipeline.py "data/Tata_Power(Sept).csv"
```

The quotation marks are important because the filename contains parentheses.

The pipeline writes a run summary and detailed quality-issue report to `outputs/`.

## Pipeline behavior and data-quality checks

The pipeline includes checks for:

- Unparseable timestamps.
- Duplicate timestamps.
- Missing load, temperature and relative-humidity values.
- Negative load and values outside configured temperature or humidity bounds.
- Missing expected time slots and unexpected timestamp intervals.
- Large changes between adjacent load readings.

A load jump is a **review flag**, not proof that the reading is incorrect. Suspicious records should be investigated rather than automatically deleted without a documented rule.

The intended table roles are:

- **`analytics.staging_observations`:** records from the current input run, including rows with quality flags.
- **`analytics.fact_observations`:** curated observations according to the pipeline's eligibility rules.
- **`analytics.data_quality_issues`:** issue records for investigation and reporting.
- **`analytics.pipeline_runs`:** run metadata, counts, duration and status.

The pipeline is designed for a **complete CSV snapshot**, not an incremental feed. A successful run refreshes staging and rebuilds the curated fact table in a transaction; it is not intended to append the same full file repeatedly.

## Recorded run

A previous run against the Tata Power CSV reported:

| Metric | Result |
|---|---:|
| Input rows | 234,272 |
| Staging rows | 234,272 |
| Eligible rows reported by pipeline | 234,272 |
| Duplicate timestamp rows | 0 |
| Missing-value rows | 0 |
| Invalid-range rows | 115 |
| Load-jump rows | 36 |
| Missing intervals reported | 70,176 |
| Unexpected intervals | 1 |
| Invalid datetime rows | 0 |
| Total issues reported | 70,328 |
| Status | SUCCESS |

These are recorded pipeline outputs, not a claim that the dataset has no quality problems. In particular, investigate the missing-interval count and confirm how invalid-range rows are handled before presenting the data as fully validated. A `SUCCESS` status means the run completed according to the pipeline's execution logic; it does not mean that every input row is free of issues.

## Useful SQL checks

Run these queries in pgAdmin's Query Tool after selecting `electricity_analytics`.

### Count curated observations and check date coverage

```sql
SELECT
    COUNT(*) AS total_rows,
    MIN(observed_at) AS first_timestamp,
    MAX(observed_at) AS last_timestamp
FROM analytics.fact_observations;
```

### Inspect the highest-demand timestamps

```sql
SELECT observed_at, load_mw
FROM analytics.fact_observations
ORDER BY load_mw DESC
LIMIT 20;
```

### Explore associations between load and weather

```sql
SELECT
    CORR(load_mw, temp_c) AS load_temp_corr,
    CORR(load_mw, relative_humidity) AS load_humidity_corr
FROM analytics.fact_observations;
```

Correlation indicates association, not causation.

### Summarize quality issues from the latest run

```sql
SELECT issue_type, severity, COUNT(*) AS issue_count
FROM analytics.v_latest_run_issues
GROUP BY issue_type, severity
ORDER BY issue_count DESC;
```

### Review pipeline execution history

```sql
SELECT *
FROM analytics.v_pipeline_run_summary
ORDER BY started_at DESC;
```

## Power BI report

Connect Power BI Desktop to PostgreSQL using **Get data → PostgreSQL database** (not the Microsoft SQL Server connector).

| Setting | Value |
|---|---|
| Server | `localhost:5432` |
| Database | `electricity_analytics` |
| Mode | Import |

Load the relevant views, including:

- `v_daily_demand`
- `v_monthly_demand`
- `v_hourly_profile`
- `v_month_hour_profile`
- `v_pipeline_run_summary`
- `v_issue_counts_by_run`
- `v_latest_run_issues`
- `v_flagged_anomalies`

The current demand dashboard includes yearly load demand, monthly load demand, hourly demand, monthly temperature and monthly relative humidity. A useful next enhancement is a dedicated **Data Quality Monitoring** page with issue counts, pipeline run history and flagged records.

**Important:** `v_daily_demand.completeness_pct` assumes 96 intervals per day for 15-minute data. If the dataset is hourly, the divisor should be 24. Verify the source frequency before interpreting completeness percentages.

## Troubleshooting

- **Connection refused:** Check that the PostgreSQL service is running and that the host, port and database name are correct.
- **Authentication failure in Power BI:** Select the PostgreSQL connector and use the PostgreSQL username/password configured for the server. Clear saved Power BI data-source permissions if necessary, then reconnect.
- **Many missing-interval flags:** Verify `EXPECTED_FREQUENCY`, timestamp consistency and the source's actual sampling cadence.
- **No curated rows:** Inspect `outputs/quality_issues_<run_id>.csv` and check timestamp parsing, duplicates and load validation rules.
- **A table or view is not found:** Confirm that pgAdmin is connected to `electricity_analytics` and that the object exists in the `analytics` schema.
- **Duplicate tables in Power BI:** In Transform data, check for repeated imports of the same view and remove only confirmed redundant queries.

## Next improvements

- Verify timestamp frequency and missing-interval calculations.
- Make accepted, rejected and flagged record handling explicit and testable.
- Add automated tests for validation rules.
- Build a dedicated data-quality and pipeline-monitoring report page.
- Add a simple architecture diagram and dashboard screenshots.
- Document the findings supported by the data.
- Consider scheduled execution and incremental loading only after the current snapshot pipeline is validated.

## Security and data sharing

- Never commit `.env`, database passwords, tokens or other secrets.
- Confirm that you have permission to share the Tata Power dataset before publishing it.
- If redistribution is not permitted, exclude the raw dataset from GitHub and provide a small synthetic sample instead.
- Do not claim a feature is implemented until it has been tested in the code and database.
