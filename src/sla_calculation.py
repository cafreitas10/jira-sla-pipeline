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
import requests
from functools import lru_cache


def _fetch_public_holidays_for_year(year, country_code='BR'):
    """Fetch public holidays for a given year and country using Nager.Date API.

    Returns a set of `date` objects. Raises on HTTP errors.
    """
    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    holidays = {pd.to_datetime(h['date']).date() for h in resp.json()}
    return holidays


@lru_cache(maxsize=8)
def get_public_holidays(years_tuple, country_code='BR'):
    """Get a set of public holiday dates for the given years and country_code.

    `years_tuple` is a tuple of int years. Results are cached for performance.
    """
    holidays = set()
    for y in years_tuple:
        try:
            holidays.update(_fetch_public_holidays_for_year(y, country_code))
        except Exception as e:
            logger.warning(f"Failed to fetch holidays for {y}-{country_code}: {e}")
    return holidays


def calculate_business_hours(start_ts, end_ts, country_code='BR'):
    """Calculate elapsed business hours between two timestamps.

    Business hours definition for this project:
    - All 24 hours of a weekday (Mon-Fri) are considered business hours.
    - Weekends (Sat/Sun) are excluded.
    - National public holidays (for the provided country_code) are excluded.

    The function counts the number of hours that lie on business days between
    `start_ts` and `end_ts`, including partial days.
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

    total_seconds = 0

    start_day = start_ts.date()
    end_day = end_ts.date()

    # Prepare years range to fetch holidays for
    years = tuple(range(start_day.year, end_day.year + 1))
    holidays = get_public_holidays(years, country_code=country_code)

    current_day = start_day
    while current_day <= end_day:
        is_weekend = current_day.weekday() >= 5
        is_holiday = current_day in holidays

        if not is_weekend and not is_holiday:
            # business day: count overlap of [start_ts, end_ts] with current_day (00:00-24:00)
            window_start = datetime.combine(current_day, time(0, 0))
            window_end = datetime.combine(current_day, time(23, 59, 59, 999999))

            period_start = max(start_ts, window_start)
            period_end = min(end_ts, window_end)

            if period_end > period_start:
                total_seconds += (period_end - period_start).total_seconds()

        current_day = current_day + timedelta(days=1)

    hours = round(total_seconds / 3600, 2)
    return hours


def add_resolution_business_hours(df_issues, country_code='BR'):
    """Add `resolution_business_hours` column to issues dataframe.

    Normalizes `created_at` and `resolved_at` to UTC-naive datetimes to avoid
    errors when some timestamps are timezone-aware and others are naive.
    Uses `calculate_business_hours(..., country_code=country_code)` to exclude
    weekends and national holidays for the specified country.
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
        lambda r: calculate_business_hours(r['created_at'], r['resolved_at'], country_code=country_code),
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


# Orchestration moved to `src/run_full_pipeline.py`.
# This module now contains only SLA calculation helpers (business-hour aware),
# holiday resolution and related dataframe utilities.
