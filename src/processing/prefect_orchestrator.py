import sys
import time
import traceback
from datetime import datetime

from prefect import flow, get_run_logger
from ingest_to_bronze import run_ingestion_bronze
from clean_transform_to_silver import run_silver_transformation as silver_transformation_flow

@flow(name="ETL Pipeline Orchestrator", log_prints=True)
def run_pipeline():
    """
    Execute the ETL pipeline using Prefect Subflows.
    - The Silver transformation will ONLY run if Bronze successfully passes its string count validations.
    """
    logger = get_run_logger()

    logger.info("ETL PIPELINE ORCHESTRATOR STARTED")
    
    # 1. Run Bronze (Includes strict Row Count validation)
    logger.info("# STEP 1: BRONZE LAYER INGESTION AND VALIDATION")
    run_ingestion_bronze()
    logger.info("[OK] Bronze ingestion completed successfully!")
    
    # 2. Run Silver
    logger.info("# STEP 2: SILVER LAYER TRANSFORMATION")
    silver_transformation_flow()
    logger.info("[OK] Silver transformation completed successfully!")
    
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")

    

if __name__ == "__main__":
    run_pipeline()
