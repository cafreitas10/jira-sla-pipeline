"""
JIRA SLA Data Engineering Pipeline - Main Orchestration.

Orchestrates the complete pipeline: Bronze → Silver → Gold layers.
"""

import logging
import sys
from bronze.ingest_bronze import main as ingest_bronze
from silver.transform_silver import main as transform_silver
from gold.build_gold import main as build_gold

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
