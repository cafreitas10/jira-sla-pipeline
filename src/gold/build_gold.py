"""
Gold layer: Build analytical reports and aggregations.

This module reads transformed data from silver layer and creates
business analytics reports for SLA compliance analysis.
"""

import os
import pandas as pd
import logging
from sla_calculation import add_resolution_business_hours, attach_sla_compliance, filter_completed_issues

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
    Build SLA compliance report for individual issues (includes SLA business-hours).

    Args:
        df_issues (pd.DataFrame): Silver layer issues dataframe.

    Returns:
        pd.DataFrame: Individual issue SLA compliance report.
    """
    try:
        # Add business-hours resolution and SLA compliance
        df = add_resolution_business_hours(df_issues)
        df = attach_sla_compliance(df)

        df_gold = df[[
            'issue_id',
            'issue_type',
            'status',
            'priority',
            'assignee_name',
            'created_at',
            'resolved_at',
            'resolution_business_hours',
            'sla_expected_hours',
            'is_sla_met'
        ]].copy()

        logger.info(f"Built {df_gold.shape[0]} SLA records (with business-hours)")
        return df_gold

    except Exception as error:
        logger.error(f"Failed to build SLA issues: {error}")
        return pd.DataFrame()


def build_gold_sla_parquet(df_issues, output_path="data/gold/SLA_by_Issue.parquet"):
    """
    Build and save SLA by Issue as a Parquet file containing only completed issues.

    The final table contains the following fields at minimum:
    - issue_id
    - issue_type
    - assignee_name
    - priority
    - created_at
    - resolved_at
    - resolution_business_hours
    - sla_expected_hours
    - is_sla_met

    Only issues with status 'Done' or 'Resolved' are included.

    Returns:
        pd.DataFrame: The saved dataframe (or empty DataFrame on failure)
    """
    try:
        df_completed = filter_completed_issues(df_issues)

        if df_completed.empty:
            logger.warning("No completed issues to write SLA_by_Issue parquet")
            return pd.DataFrame()

        df_completed = add_resolution_business_hours(df_completed)
        df_completed = attach_sla_compliance(df_completed)

        df_final = df_completed[[
            'issue_id',
            'issue_type',
            'assignee_name',
            'priority',
            'created_at',
            'resolved_at',
            'resolution_business_hours',
            'sla_expected_hours',
            'is_sla_met'
        ]].copy()

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_final.to_parquet(output_path, index=False)
        logger.info(f"Saved SLA_by_Issue parquet: {output_path}")
        return df_final

    except Exception as error:
        logger.error(f"Failed to build SLA_by_Issue parquet: {error}")
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


def main():
    """
    Main pipeline to build the two required gold CSV reports from the
    intermediate `SLA_by_Issue.parquet` file.

    Returns:
        bool: True if reports created successfully, False otherwise.
    """
    silver_path = "data/silver/silver_issues.parquet"

    # Read silver layer
    df_issues = read_silver_issues(silver_path)

    if df_issues.empty:
        logger.error("No data to build gold layer")
        return False

    # Build SLA by Issue as Parquet (only completed issues)
    df_sla_parquet = build_gold_sla_parquet(df_issues, "data/gold/SLA_by_Issue.parquet")

    if df_sla_parquet.empty:
        logger.warning("No completed issues to build average SLA reports")
        return False

    # 1) Average SLA by Analyst (includes summed resolution_hours)
    df_analyst = (
        df_sla_parquet.groupby('assignee_name').agg(
            issue_count=('issue_id', 'count'),
            resolution_hours=('resolution_business_hours', 'sum'),
            sla_avg_hours=('resolution_business_hours', 'mean')
        )
        .reset_index()
    )
    df_analyst['SLA médio (em horas)'] = df_analyst['sla_avg_hours'].round(2)
    df_analyst['resolution_hours'] = df_analyst['resolution_hours'].round(2)
    df_analyst = df_analyst[['assignee_name', 'issue_count', 'resolution_hours', 'SLA médio (em horas)']]
    df_analyst.columns = ['Analista', 'Quantidade de chamados', 'resolution_hours', 'SLA médio (em horas)']

    os.makedirs(os.path.dirname("data/gold/average_sla_by_analist.csv"), exist_ok=True)
    df_analyst.to_csv("data/gold/average_sla_by_analist.csv", index=False)

    # 2) Average SLA by Issue Type (includes summed resolution_hours)
    df_issue_type = (
        df_sla_parquet.groupby('issue_type').agg(
            issue_count=('issue_id', 'count'),
            resolution_hours=('resolution_business_hours', 'sum'),
            sla_avg_hours=('resolution_business_hours', 'mean')
        )
        .reset_index()
    )
    df_issue_type['SLA médio (em horas)'] = df_issue_type['sla_avg_hours'].round(2)
    df_issue_type['resolution_hours'] = df_issue_type['resolution_hours'].round(2)

    df_issue_type = df_issue_type[['issue_type', 'issue_count', 'resolution_hours', 'SLA médio (em horas)']]
    df_issue_type.columns = ['Tipo do chamado', 'Quantidade de chamados', 'resolution_hours', 'SLA médio (em horas)']

    df_issue_type.to_csv("data/gold/average_sla_by_issue_type.csv", index=False)

    logger.info("Gold layer pipeline completed successfully")
    return True


if __name__ == "__main__":
    main()
