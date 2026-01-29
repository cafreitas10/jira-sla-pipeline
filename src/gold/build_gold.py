"""
Gold layer: Build analytical reports and aggregations.

This module reads transformed data from silver layer and creates
business analytics reports for SLA compliance analysis.
"""

import os
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def read_silver_issues(silver_path):
    """
    Read issues from silver layer.

    Args:
        silver_path (str): Path to silver parquet file.

    Returns:
        pd.DataFrame: Issues dataframe or empty dataframe if failed.
    """
    try:
        df_issues = pd.read_parquet(silver_path)
        logger.info(f"Read {df_issues.shape[0]} issues from silver layer")
        return df_issues

    except Exception as error:
        logger.error(f"Failed to read silver layer: {error}")
        return pd.DataFrame()


def build_gold_sla_issues(df_issues):
    """
    Build SLA compliance report for individual issues.

    Args:
        df_issues (pd.DataFrame): Silver layer issues dataframe.

    Returns:
        pd.DataFrame: Individual issue SLA compliance report.
    """
    try:
        df_gold = df_issues[[
            'issue_id',
            'issue_type',
            'status',
            'priority',
            'assignee_name',
            'created_at',
            'resolved_at',
            'resolution_hours',
            'sla_expected_hours'
        ]].copy()

        # Check SLA compliance (boolean column with is_ prefix)
        df_gold['is_sla_met'] = (
            df_gold['resolution_hours'] <= df_gold['sla_expected_hours']
        )

        logger.info(f"Built {df_gold.shape[0]} SLA records")
        return df_gold

    except Exception as error:
        logger.error(f"Failed to build SLA issues: {error}")
        return pd.DataFrame()


def build_gold_sla_by_analyst(df_issues):
    """
    Build SLA summary report aggregated by analyst.

    Args:
        df_issues (pd.DataFrame): Silver layer issues dataframe.

    Returns:
        pd.DataFrame: SLA summary by analyst.
    """
    try:
        df_gold = df_issues.groupby('assignee_name').agg({
            'issue_id': 'count',
            'resolution_hours': ['mean', 'max', 'min'],
            'sla_expected_hours': 'mean'
        }).round(2)

        # Flatten multi-level columns
        df_gold.columns = [
            'total_issues',
            'avg_resolution_hours',
            'max_resolution_hours',
            'min_resolution_hours',
            'avg_sla_expected_hours'
        ]

        df_gold = df_gold.reset_index()

        # Calculate SLA compliance percentage
        df_compliance = df_issues.groupby('assignee_name').apply(
            lambda x: (
                (x['resolution_hours'] <= x['sla_expected_hours']).sum() / len(x) * 100
            )
        ).round(2)

        df_gold['sla_compliance_percentage'] = df_gold['assignee_name'].map(df_compliance)

        logger.info(f"Built SLA summary for {df_gold.shape[0]} analysts")
        return df_gold

    except Exception as error:
        logger.error(f"Failed to build SLA by analyst: {error}")
        return pd.DataFrame()


def build_gold_sla_by_issue_type(df_issues):
    """
    Build SLA summary report aggregated by issue type.

    Args:
        df_issues (pd.DataFrame): Silver layer issues dataframe.

    Returns:
        pd.DataFrame: SLA summary by issue type.
    """
    try:
        df_gold = df_issues.groupby('issue_type').agg({
            'issue_id': 'count',
            'resolution_hours': ['mean', 'max', 'min'],
            'sla_expected_hours': 'mean'
        }).round(2)

        # Flatten multi-level columns
        df_gold.columns = [
            'total_issues',
            'avg_resolution_hours',
            'max_resolution_hours',
            'min_resolution_hours',
            'avg_sla_expected_hours'
        ]

        df_gold = df_gold.reset_index()

        # Calculate SLA compliance percentage
        df_compliance = df_issues.groupby('issue_type').apply(
            lambda x: (
                (x['resolution_hours'] <= x['sla_expected_hours']).sum() / len(x) * 100
            )
        ).round(2)

        df_gold['sla_compliance_percentage'] = df_gold['issue_type'].map(df_compliance)

        logger.info(f"Built SLA summary for {df_gold.shape[0]} issue types")
        return df_gold

    except Exception as error:
        logger.error(f"Failed to build SLA by issue type: {error}")
        return pd.DataFrame()


def save_gold_layer(df_gold, output_path):
    """
    Save gold layer report to CSV format.

    Args:
        df_gold (pd.DataFrame): Gold layer dataframe.
        output_path (str): Path to save CSV file.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_gold.to_csv(output_path, index=False)
        logger.info(f"Gold layer saved: {output_path}")
        return True

    except Exception as error:
        logger.error(f"Failed to save gold layer: {error}")
        return False


def main():
    """
    Main pipeline to build all gold layer reports.

    Returns:
        bool: True if all reports created successfully, False otherwise.
    """
    silver_path = "data/silver/silver_issues.parquet"

    # Read silver layer
    df_issues = read_silver_issues(silver_path)

    if df_issues.empty:
        logger.error("No data to build gold layer")
        return False

    # Build SLA issues report
    df_sla_issues = build_gold_sla_issues(df_issues)
    save_gold_layer(df_sla_issues, "data/gold/gold_sla_issues.csv")

    # Build SLA by analyst report
    df_sla_analyst = build_gold_sla_by_analyst(df_issues)
    save_gold_layer(df_sla_analyst, "data/gold/gold_sla_by_analyst.csv")

    # Build SLA by issue type report
    df_sla_type = build_gold_sla_by_issue_type(df_issues)
    save_gold_layer(df_sla_type, "data/gold/gold_sla_by_issue_type.csv")

    logger.info("Gold layer pipeline completed successfully")
    return True


if __name__ == "__main__":
    main()
