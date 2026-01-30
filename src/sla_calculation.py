"""
JIRA SLA Data Engineering Pipeline - Main Orchestration.

Orchestrates the complete pipeline: Bronze → Silver → Gold layers.
"""

import logging
import sys
# Defer pipeline stage imports to runtime to avoid importing heavy deps during unit tests
# (e.g., azure SDK) which are not required to test SLA helper functions.

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ---- SLA calculation helpers (business-hours) ----
import pandas as pd
from datetime import datetime, time, timedelta


def calculate_business_hours(start_ts, end_ts, workday_start=9, workday_end=17):
    """Calculate elapsed business hours between two timestamps.

    Assumptions:
    - Business days are Monday-Friday
    - Business hours run from `workday_start` (inclusive) to `workday_end` (exclusive)
      (default: 09:00 - 17:00, 8 hours/day)
    """

    if pd.isna(start_ts) or pd.isna(end_ts):
        return pd.NA

    # Ensure datetimes
    if not isinstance(start_ts, datetime):
        start_ts = pd.to_datetime(start_ts).to_pydatetime()
    if not isinstance(end_ts, datetime):
        end_ts = pd.to_datetime(end_ts).to_pydatetime()

    if end_ts <= start_ts:
        return 0.0

    day_start_time = time(hour=workday_start)
    day_end_time = time(hour=workday_end)

    total_seconds = 0
    current_day = start_ts.date()
    last_day = end_ts.date()

    while current_day <= last_day:
        if current_day.weekday() < 5:  # Mon-Fri
            window_start = datetime.combine(current_day, day_start_time)
            window_end = datetime.combine(current_day, day_end_time)

            period_start = max(start_ts, window_start)
            period_end = min(end_ts, window_end)

            if period_end > period_start:
                total_seconds += (period_end - period_start).total_seconds()

        current_day = current_day + timedelta(days=1)

    hours = round(total_seconds / 3600, 2)
    return hours


def add_resolution_business_hours(df_issues, workday_start=9, workday_end=17):
    """Add `resolution_business_hours` column to issues dataframe.

    Normalizes `created_at` and `resolved_at` to UTC-naive datetimes to avoid
    errors when some timestamps are timezone-aware and others are naive.
    """
    df = df_issues.copy()

    # Parse timestamps as UTC and then remove tzinfo (make naive UTC) for consistent arithmetic
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
    df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce', utc=True)

    # Ensure we have timezone-naive datetimes (all in UTC)
    try:
        df['created_at'] = df['created_at'].dt.tz_convert(None)
        df['resolved_at'] = df['resolved_at'].dt.tz_convert(None)
    except Exception:
        # If conversion fails, attempt to localize naive (fallback)
        df['created_at'] = df['created_at'].dt.tz_localize(None)
        df['resolved_at'] = df['resolved_at'].dt.tz_localize(None)

    df['resolution_business_hours'] = df.apply(
        lambda r: calculate_business_hours(r['created_at'], r['resolved_at'], workday_start, workday_end),
        axis=1
    )

    logger.info("Added resolution_business_hours to dataframe")
    return df


def attach_sla_compliance(df_issues, resolution_col='resolution_business_hours', sla_col='sla_expected_hours'):
    """Attach `is_sla_met` boolean column comparing resolution and expected SLA."""
    df = df_issues.copy()
    df['is_sla_met'] = (
        df[resolution_col].notna() & df[sla_col].notna() &
        (df[resolution_col] <= df[sla_col])
    )

    logger.info("Attached is_sla_met indicator to dataframe")
    return df


def filter_completed_issues(df_issues, completed_statuses=None):
    """Return only issues whose status indicates they are finished.

    By default, uses statuses 'Done' and 'Resolved'.
    """
    if completed_statuses is None:
        completed_statuses = {'Done', 'Resolved'}

    df = df_issues.copy()
    if 'status' not in df.columns:
        logger.warning("No 'status' column found when filtering completed issues")
        return df

    mask = df['status'].isin(completed_statuses)
    filtered = df[mask].reset_index(drop=True)
    logger.info(f"Filtered completed issues: {filtered.shape[0]} rows")
    return filtered


def run_full_pipeline():
    """
    Run complete data pipeline: Bronze → Silver → Gold.

    Pipeline stages:
    - Bronze: Ingest raw data from Azure Blob Storage
    - Silver: Parse, flatten, calculate metrics
    - Gold: Build analytical reports and aggregations

    Returns:
        bool: True if all stages successful, False otherwise.
    """
    logger.info("=" * 60)
    logger.info("Starting JIRA SLA Data Engineering Pipeline")
    logger.info("=" * 60)

    # Stage 1: Bronze (Ingest)
    logger.info("\n[STAGE 1] Bronze Layer - Ingesting raw data from Azure Blob")
    # Import pipeline stages here to avoid heavy imports during module import
    from bronze.ingest_bronze import main as ingest_bronze
    from silver.transform_silver import main as transform_silver
    from gold.build_gold import main as build_gold

    if not ingest_bronze():
        logger.error("Bronze layer ingest failed. Stopping pipeline.")
        return False

    # Stage 2: Silver (Transform + Flatten)
    logger.info("\n[STAGE 2] Silver Layer - Parsing, flattening and calculating metrics")
    if not transform_silver():
        logger.error("Silver layer transformation failed. Stopping pipeline.")
        return False

    # Stage 3: Gold (Aggregate)
    logger.info("\n[STAGE 3] Gold Layer - Building analytical reports")
    if not build_gold():
        logger.error("Gold layer build failed. Stopping pipeline.")
        return False

    logger.info("\n" + "=" * 60)
    logger.info("✅ Pipeline completed successfully!")
    logger.info("=" * 60)
    return True


if __name__ == "__main__":
    success = run_full_pipeline()
    sys.exit(0 if success else 1)
