"""Electricity demand ETL + data-quality pipeline.

Usage (from the repository root):
    python src/pipeline.py data/raw/load_weather.csv

This starter treats the CSV as a complete snapshot: staging is refreshed and the
curated fact table is rebuilt transactionally on each successful run. The CSV is
never edited. Suspect readings are flagged; load jumps are retained in the fact
layer for investigation rather than silently removed.
"""
from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
OUTPUTS_DIR = ROOT / "outputs"
DEFAULT_CSV = ROOT / "data" / "raw" / "load_weather.csv"

ALIASES = {
    "datetime": "datetime",
    "timestamp": "datetime",
    "datetimeutc": "datetime",
    "dateandtime": "datetime",
    "wdload": "load_mw",
    "load": "load_mw",
    "loadmw": "load_mw",
    "demand": "load_mw",
    "temp": "temp_c",
    "temperature": "temp_c",
    "tempc": "temp_c",
    "rh": "relative_humidity",
    "humidity": "relative_humidity",
    "relativehumidity": "relative_humidity",
}


def canonical_column(value: str) -> str:
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def load_source(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV not found: {csv_path}\nPut your file at {DEFAULT_CSV}, "
            "or pass its path as the first command-line argument."
        )

    source = pd.read_csv(csv_path, low_memory=False)
    rename_map: dict[str, str] = {}
    for column in source.columns:
        alias = ALIASES.get(canonical_column(column))
        if alias:
            rename_map[column] = alias

    source = source.rename(columns=rename_map)
    if source.columns.duplicated().any():
        duplicates = source.columns[source.columns.duplicated()].tolist()
        raise ValueError(
            f"Multiple source columns map to the same standard field: {duplicates}. "
            "Keep one datetime, one load, one temp, and one RH column."
        )

    required = {"datetime", "load_mw", "temp_c", "relative_humidity"}
    missing_columns = required - set(source.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}. "
            "Accepted examples: datetime, wdLoad/load, temp, rh."
        )

    df = pd.DataFrame()
    df["source_row_number"] = np.arange(2, len(source) + 2, dtype=int)  # header is line 1
    df["raw_datetime"] = source["datetime"].astype("string").fillna("")

    parsed = pd.to_datetime(source["datetime"], errors="coerce")
    # This project assumes the source's timestamps are local, timezone-naive values.
    # If your file has explicit time zones, normalize it to one time zone first.
    try:
        if parsed.dt.tz is not None:
            parsed = parsed.dt.tz_localize(None)
    except (AttributeError, TypeError):
        pass
    df["observed_at"] = parsed

    for col in ["load_mw", "temp_c", "relative_humidity"]:
        df[col] = pd.to_numeric(source[col], errors="coerce")
        df[col] = df[col].replace([np.inf, -np.inf], np.nan)

    return df


def add_quality_flags(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]], dict[str, int]]:
    load_dotenv(ROOT / ".env")
    expected_frequency_text = os.getenv("EXPECTED_FREQUENCY", "15min")
    expected_frequency = pd.Timedelta(expected_frequency_text)
    if expected_frequency <= pd.Timedelta(0):
        raise ValueError("EXPECTED_FREQUENCY must be a positive interval such as 15min or 1h.")

    min_temp = float(os.getenv("MIN_TEMP_C", "-10"))
    max_temp = float(os.getenv("MAX_TEMP_C", "60"))
    jump_min_mw = float(os.getenv("LOAD_JUMP_MIN_MW", "50"))
    jump_pct = float(os.getenv("LOAD_JUMP_PCT", "0.20"))

    df = df.copy()
    df["is_duplicate_timestamp"] = False
    df["has_missing_value"] = df[["load_mw", "temp_c", "relative_humidity"]].isna().any(axis=1)
    df["invalid_load"] = df["load_mw"].notna() & (df["load_mw"] < 0)
    df["invalid_temp"] = df["temp_c"].notna() & ~df["temp_c"].between(min_temp, max_temp)
    df["invalid_humidity"] = df["relative_humidity"].notna() & ~df["relative_humidity"].between(0, 100)
    df["is_load_jump"] = False

    valid_time_mask = df["observed_at"].notna()
    duplicate_mask = valid_time_mask & df.loc[valid_time_mask, "observed_at"].duplicated(keep=False).reindex(df.index, fill_value=False)
    df.loc[duplicate_mask, "is_duplicate_timestamp"] = True

    # Large changes are flagged for investigation, not deleted from the source.
    jump_candidates = df.loc[
        valid_time_mask
        & ~df["is_duplicate_timestamp"]
        & df["load_mw"].notna()
        & ~df["invalid_load"]
    ].sort_values("observed_at")
    if len(jump_candidates) > 1:
        previous_load = jump_candidates["load_mw"].shift(1)
        absolute_change = (jump_candidates["load_mw"] - previous_load).abs()
        threshold = np.maximum(jump_min_mw, previous_load.abs() * jump_pct)
        jumps = absolute_change.gt(threshold) & previous_load.notna()
        df.loc[jump_candidates.index[jumps], "is_load_jump"] = True

    issues: list[dict[str, Any]] = []

    def add_issue(row_number: int | None, observed_at: Any, issue_type: str,
                  severity: str, details: str) -> None:
        if pd.isna(observed_at):
            observed_at = None
        elif isinstance(observed_at, pd.Timestamp):
            observed_at = observed_at.to_pydatetime()
        issues.append({
            "source_row_number": row_number,
            "observed_at": observed_at,
            "issue_type": issue_type,
            "severity": severity,
            "details": details,
        })

    for idx, row in df.iterrows():
        row_num = int(row["source_row_number"])
        ts = row["observed_at"]
        if pd.isna(ts):
            add_issue(row_num, None, "invalid_datetime", "HIGH",
                      f"Could not parse datetime value {row['raw_datetime']!r}.")

        if bool(row["is_duplicate_timestamp"]):
            add_issue(row_num, ts, "duplicate_timestamp", "HIGH",
                      "More than one source row has this timestamp; all rows at this timestamp are excluded from the curated fact table until reconciled.")

        for field, label in [
            ("load_mw", "load"),
            ("temp_c", "temperature"),
            ("relative_humidity", "relative_humidity"),
        ]:
            if pd.isna(row[field]):
                add_issue(row_num, ts, f"missing_{label}", "MEDIUM",
                          f"The {label} measurement is blank or could not be converted to a number.")

        if bool(row["invalid_load"]):
            add_issue(row_num, ts, "invalid_load", "HIGH",
                      f"Load must be non-negative; found {row['load_mw']} MW.")
        if bool(row["invalid_temp"]):
            add_issue(row_num, ts, "invalid_temp", "MEDIUM",
                      f"Temperature {row['temp_c']} C is outside configured bounds [{min_temp}, {max_temp}] C.")
        if bool(row["invalid_humidity"]):
            add_issue(row_num, ts, "invalid_humidity", "MEDIUM",
                      f"Relative humidity {row['relative_humidity']} is outside the valid 0-100 range.")
        if bool(row["is_load_jump"]):
            add_issue(row_num, ts, "load_jump", "REVIEW",
                      f"Load changed sharply relative to the prior valid reading: {row['load_mw']} MW. Review against operations/events; the value is retained.")

    # Compare timestamps with the configured cadence. A long gap can produce both
    # a missing-slot issue and an unexpected-interval issue; these answer different
    # questions (which points are absent versus where spacing deviates from cadence).
    observed_times = sorted(pd.Timestamp(t) for t in df.loc[valid_time_mask, "observed_at"].dropna().unique())
    missing_intervals: list[pd.Timestamp] = []
    unexpected_intervals = 0
    if len(observed_times) >= 2:
        expected_times = pd.date_range(observed_times[0], observed_times[-1], freq=expected_frequency)
        observed_set = set(observed_times)
        missing_intervals = [ts for ts in expected_times if ts not in observed_set]
        for previous, current in zip(observed_times[:-1], observed_times[1:]):
            delta = current - previous
            if delta != expected_frequency:
                unexpected_intervals += 1
                add_issue(None, current, "unexpected_interval", "MEDIUM",
                          f"Interval from {previous} to {current} is {delta}; expected one interval of {expected_frequency_text}. Check for missing records or a cadence change.")

    for missing_ts in missing_intervals:
        add_issue(None, missing_ts, "missing_interval", "MEDIUM",
                  f"No source observation exists for expected timestamp {missing_ts} at frequency {expected_frequency_text}.")

    counts = {
        "input_rows": int(len(df)),
        "duplicate_timestamp_rows": int(df["is_duplicate_timestamp"].sum()),
        "missing_value_rows": int(df["has_missing_value"].sum()),
        "invalid_range_rows": int((df["invalid_load"] | df["invalid_temp"] | df["invalid_humidity"]).sum()),
        "load_jump_rows": int(df["is_load_jump"].sum()),
        "missing_interval_count": int(len(missing_intervals)),
        "unexpected_interval_count": int(unexpected_intervals),
        "invalid_datetime_rows": int(df["observed_at"].isna().sum()),
        "issue_count": int(len(issues)),
    }
    return df, issues, counts


def safe_float(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def main() -> int:
    load_dotenv(ROOT / ".env")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL is missing. Copy .env.example to .env and edit it.", file=sys.stderr)
        return 2

    csv_path = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else DEFAULT_CSV
    if not csv_path.is_absolute():
        csv_path = (Path.cwd() / csv_path).resolve()

    run_id = uuid.uuid4()
    started_at = datetime.now(timezone.utc)
    clock_start = time.perf_counter()
    source_label = str(csv_path)
    engine = create_engine(database_url, pool_pre_ping=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # Write a RUNNING marker separately so failures in the ETL transaction can
        # still be recorded as FAILED.
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO analytics.pipeline_runs (run_id, source_file, started_at, status)
                VALUES (:run_id, :source_file, :started_at, 'RUNNING')
            """), {"run_id": run_id, "source_file": source_label, "started_at": started_at})

        source_df = load_source(csv_path)
        checked_df, issues, counts = add_quality_flags(source_df)

        # Curated eligibility rules:
        # - timestamp parsed;
        # - unique timestamp within this file;
        # - load exists and is not negative.
        # Valid load observations are retained even if a weather field is missing
        # or invalid; invalid weather fields become NULL in the curated table.
        eligible_mask = (
            checked_df["observed_at"].notna()
            & ~checked_df["is_duplicate_timestamp"]
            & checked_df["load_mw"].notna()
            & ~checked_df["invalid_load"]
        )
        eligible = checked_df.loc[eligible_mask].copy()
        eligible["curated_temp_c"] = eligible["temp_c"].where(~eligible["invalid_temp"], np.nan)
        eligible["curated_rh"] = eligible["relative_humidity"].where(~eligible["invalid_humidity"], np.nan)

        stage_cols = [
            "source_row_number", "raw_datetime", "observed_at", "load_mw", "temp_c",
            "relative_humidity", "is_duplicate_timestamp", "has_missing_value",
            "invalid_load", "invalid_temp", "invalid_humidity", "is_load_jump",
        ]
        stage_df = checked_df[stage_cols].copy()
        stage_df.insert(0, "run_id", run_id)

        issue_df = pd.DataFrame(issues, columns=[
            "source_row_number", "observed_at", "issue_type", "severity", "details"
        ])
        issue_export = issue_df.copy()
        if not issue_export.empty:
            issue_export.insert(0, "run_id", str(run_id))

        # Main ETL transaction. If any step fails, staging/fact/issues changes roll back.
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM analytics.staging_observations"))
            stage_df.to_sql(
                "staging_observations", con=conn, schema="analytics", if_exists="append",
                index=False, method="multi", chunksize=1000,
            )

            conn.execute(text("DELETE FROM analytics.fact_observations"))
            fact_records = []
            for _, row in eligible.iterrows():
                observed_at = row["observed_at"]
                if isinstance(observed_at, pd.Timestamp):
                    observed_at = observed_at.to_pydatetime()
                fact_records.append({
                    "observed_at": observed_at,
                    "load_mw": float(row["load_mw"]),
                    "temp_c": safe_float(row["curated_temp_c"]),
                    "relative_humidity": safe_float(row["curated_rh"]),
                    "source_row_number": int(row["source_row_number"]),
                    "is_load_jump": bool(row["is_load_jump"]),
                    "last_updated_run": run_id,
                })

            if fact_records:
                conn.execute(text("""
                    INSERT INTO analytics.fact_observations
                        (observed_at, load_mw, temp_c, relative_humidity,
                         source_row_number, is_load_jump, last_updated_run)
                    VALUES
                        (:observed_at, :load_mw, :temp_c, :relative_humidity,
                         :source_row_number, :is_load_jump, :last_updated_run)
                """), fact_records)

            issue_records = []
            for issue in issues:
                issue_records.append({"run_id": run_id, **issue})
            if issue_records:
                conn.execute(text("""
                    INSERT INTO analytics.data_quality_issues
                        (run_id, source_row_number, observed_at, issue_type, severity, details)
                    VALUES
                        (:run_id, :source_row_number, :observed_at, :issue_type, :severity, :details)
                """), issue_records)

            duration = time.perf_counter() - clock_start
            conn.execute(text("""
                UPDATE analytics.pipeline_runs
                SET completed_at = NOW(), status = 'SUCCESS',
                    input_rows = :input_rows,
                    staging_rows = :staging_rows,
                    eligible_rows = :eligible_rows,
                    duplicate_timestamp_rows = :duplicate_timestamp_rows,
                    missing_value_rows = :missing_value_rows,
                    invalid_range_rows = :invalid_range_rows,
                    load_jump_rows = :load_jump_rows,
                    missing_interval_count = :missing_interval_count,
                    unexpected_interval_count = :unexpected_interval_count,
                    invalid_datetime_rows = :invalid_datetime_rows,
                    issue_count = :issue_count,
                    duration_seconds = :duration_seconds,
                    error_message = NULL
                WHERE run_id = :run_id
            """), {**counts, "staging_rows": len(stage_df), "eligible_rows": len(fact_records),
                   "duration_seconds": duration, "run_id": run_id})

        summary = {
            "run_id": str(run_id),
            "source_file": source_label,
            **counts,
            "staging_rows": len(stage_df),
            "eligible_rows": len(fact_records),
            "duration_seconds": round(time.perf_counter() - clock_start, 3),
            "status": "SUCCESS",
        }
        pd.DataFrame([summary]).to_csv(OUTPUTS_DIR / f"run_summary_{run_id}.csv", index=False)
        issue_export.to_csv(OUTPUTS_DIR / f"quality_issues_{run_id}.csv", index=False)

        print("\nPIPELINE COMPLETED")
        for key, value in summary.items():
            print(f"{key}: {value}")
        print(f"\nRun summary: {OUTPUTS_DIR / f'run_summary_{run_id}.csv'}")
        print(f"Quality issues: {OUTPUTS_DIR / f'quality_issues_{run_id}.csv'}")
        return 0

    except Exception as exc:
        duration = time.perf_counter() - clock_start
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE analytics.pipeline_runs
                    SET completed_at = NOW(), status = 'FAILED',
                        duration_seconds = :duration_seconds, error_message = :error_message
                    WHERE run_id = :run_id
                """), {"run_id": run_id, "duration_seconds": duration, "error_message": str(exc)[:4000]})
        except Exception as logging_exc:
            print(f"Could not write failure to pipeline_runs: {logging_exc}", file=sys.stderr)
        print(f"PIPELINE FAILED: {exc}", file=sys.stderr)
        return 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
