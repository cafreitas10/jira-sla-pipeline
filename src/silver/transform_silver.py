"""
Silver layer: Parse, flatten, and calculate business metrics.

This module reads raw JSON from bronze layer, parses nested structures,
flattens assignee and timestamp arrays, and calculates SLA-related metrics.
"""

import datetime
import os
import pandas as pd
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def read_bronze_issues(bronze_path):
    """
    Read raw JSON from bronze layer.

    Args:
        bronze_path (str): Path to bronze JSON file.

    Returns:
        dict: Parsed JSON object or empty dict if failed.
    """
    try:
        with open(bronze_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        logger.info(f"Read bronze layer from: {bronze_path}")
        return data

    except Exception as error:
        logger.error(f"Failed to read bronze layer: {error}")
        return {}


def parse_and_flatten_issues(data):
    """
    Parse JIRA JSON structure and flatten nested assignee/timestamps.

    Args:
        data (dict): Raw JIRA JSON data.

    Returns:
        pd.DataFrame: Flattened issues dataframe.
    """
    try:
        # Extract issues array
        if 'issues' not in data:
            logger.error("No 'issues' key found in JSON")
            return pd.DataFrame()

        issues_array = data['issues']

        # Flatten nested structures without using pd.json_normalize
        rows = []
        for issue in issues_array:
            issue_id = issue.get('id')
            issue_type = issue.get('issue_type')
            status = issue.get('status')
            priority = issue.get('priority')

            # Timestamps: use first element if present
            created_at = None
            resolved_at = None
            timestamps = issue.get('timestamps') or []
            if isinstance(timestamps, list) and len(timestamps) > 0:
                ts0 = timestamps[0] or {}
                created_at = ts0.get('created_at')
                resolved_at = ts0.get('resolved_at')

            assignees = issue.get('assignee') or []
            if not assignees:
                # preserve issues with no assignee as a single row with null assignee fields
                rows.append({
                    'issue_id': issue_id,
                    'issue_type': issue_type,
                    'status': status,
                    'priority': priority,
                    'assignee_id': None,
                    'assignee_name': None,
                    'assignee_email': None,
                    'created_at': created_at,
                    'resolved_at': resolved_at
                })
            else:
                for a in assignees:
                    rows.append({
                        'issue_id': issue_id,
                        'issue_type': issue_type,
                        'status': status,
                        'priority': priority,
                        'assignee_id': a.get('id'),
                        'assignee_name': a.get('name'),
                        'assignee_email': a.get('email'),
                        'created_at': created_at,
                        'resolved_at': resolved_at
                    })

        if not rows:
            logger.error("No rows created during flattening")
            return pd.DataFrame()

        df_issues = pd.DataFrame(rows)

        # Ensure column order follows naming convention
        df_issues = df_issues[[
            'issue_id',
            'issue_type',
            'status',
            'priority',
            'assignee_id',
            'assignee_name',
            'assignee_email',
            'created_at',
            'resolved_at'
        ]]

        logger.info(f"Parsed and flattened {df_issues.shape[0]} issues")
        return df_issues

    except Exception as error:
        logger.error(f"Parsing and flattening failed: {error}")
        return pd.DataFrame()


def calculate_resolution_hours(df_issues):
    """
    Calculate resolution time in hours.

    Args:
        df_issues (pd.DataFrame): Issues dataframe with timestamps.

    Returns:
        pd.DataFrame: Dataframe with resolution_hours column added.
    """
    try:
        df_issues['created_at'] = pd.to_datetime(df_issues['created_at'])
        df_issues['resolved_at'] = pd.to_datetime(df_issues['resolved_at'])

        # Calculate resolution hours (handle null resolved_at)
        df_issues['resolution_hours'] = (
            (df_issues['resolved_at'] - df_issues['created_at'])
            .dt.total_seconds() / 3600
        ).round(2)

        logger.info("Calculated resolution_hours for all issues")
        return df_issues

    except Exception as error:
        logger.error(f"Failed to calculate resolution hours: {error}")
        return df_issues


def add_sla_expected_hours(df_issues, sla_config=None):
    """
    Add expected SLA hours based on priority level.

    Args:
        df_issues (pd.DataFrame): Issues dataframe.
        sla_config (dict): SLA configuration by priority (optional).

    Returns:
        pd.DataFrame: Dataframe with sla_expected_hours column added.
    """
    if sla_config is None:
        sla_config = {
            'Critical': 4,
            'High': 8,
            'Medium': 24,
            'Low': 72
        }

    try:
        df_issues['sla_expected_hours'] = df_issues['priority'].map(sla_config)
        logger.info("Added sla_expected_hours based on priority")
        return df_issues

    except Exception as error:
        logger.error(f"Failed to add SLA expected hours: {error}")
        return df_issues


def validate_and_filter_types(df_issues):
    """
    Validate and filter rows with incorrect types or invalid values.

    - Coerce `created_at` and `resolved_at` to datetimes (invalid -> NaT)
    - Ensures `issue_id` and `priority` are present and valid
    - Rejects rows that fail validation and saves them for audit

    Returns:
        pd.DataFrame: Filtered dataframe with valid rows only.
    """
    df = df_issues.copy()

    # Coerce to expected types
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')

    # Use pandas string dtype to preserve missing values as <NA>
    df['issue_id'] = df['issue_id'].astype('string')
    df['issue_type'] = df['issue_type'].astype('string')
    df['status'] = df['status'].astype('string')
    df['priority'] = df['priority'].astype('string')

    # Assignee fields are optional, but normalize dtype
    df['assignee_id'] = df['assignee_id'].astype('string')
    df['assignee_name'] = df['assignee_name'].astype('string')
    df['assignee_email'] = df['assignee_email'].astype('string')

    # Validation rules
    required_mask = (
        df['issue_id'].notna() &
        df['created_at'].notna() &
        df['priority'].notna()
    )

    allowed_priorities = {'Critical', 'High', 'Medium', 'Low'}
    required_mask &= df['priority'].isin(allowed_priorities)

    rejected = df[~required_mask]
    if not rejected.empty:
        logger.warning(f"Rejecting {len(rejected)} rows with invalid types/values")
        os.makedirs(os.path.dirname("data/silver/rejected_rows.csv"), exist_ok=True)
        rejected.to_csv("data/silver/rejected_rows.csv", index=False)

    return df[required_mask].reset_index(drop=True)


def transform_silver_layer(df_issues):
    """
    Apply all silver layer transformations and calculations.

    Args:
        df_issues (pd.DataFrame): Flattened issues dataframe.

    Returns:
        pd.DataFrame: Transformed dataframe ready for gold layer.
    """

    # Validate types and filter invalid rows
    df_issues = validate_and_filter_types(df_issues)

    # Calculate resolution hours
    df_issues = calculate_resolution_hours(df_issues)

    # Add SLA expected hours
    df_issues = add_sla_expected_hours(df_issues)

    # Data quality checks
    logger.info(f"Silver layer shape: {df_issues.shape}")
    logger.info(f"Null resolution_hours: {df_issues['resolution_hours'].isna().sum()}")

    return df_issues


def save_silver_layer(df_issues, output_path):
    """
    Save transformed issues to silver layer in Parquet format.

    Args:
        df_issues (pd.DataFrame): Transformed issues dataframe.
        output_path (str): Path to save parquet file.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_issues.to_parquet(output_path, index=False)
        logger.info(f"Silver layer saved: {output_path}")
        return True

    except Exception as error:
        logger.error(f"Failed to save silver layer: {error}")
        return False


def main():
    """
    Main transformation pipeline for silver layer.

    Returns:
        bool: True if successful, False otherwise.
    """
    bronze_path = "data/bronze/bronze_issues.json"
    silver_path = "data/silver/silver_issues.parquet"

    # Read raw JSON from bronze layer
    data = read_bronze_issues(bronze_path)

    if not data:
        logger.error("No data to transform")
        return False

    # Parse and flatten nested structures (SILVER LAYER)
    df_issues = parse_and_flatten_issues(data)

    if df_issues.empty:
        logger.error("No issues parsed from bronze layer")
        return False

    # Transform to silver layer
    df_silver = transform_silver_layer(df_issues)

    # Save to silver layer
    success = save_silver_layer(df_silver, silver_path)

    logger.info(f"Silver layer transformation completed: {success}")
    return success


if __name__ == "__main__":
    main()
