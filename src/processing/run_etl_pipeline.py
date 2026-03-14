"""
ETL Pipeline Orchestrator

This script orchestrates the execution of the ETL pipeline:
1. Bronze Layer: Ingest raw CSV data into Delta tables
2. Silver Layer: Clean and transform data from Bronze to Silver

Usage:
    python run_etl_pipeline.py          # Run full pipeline
    python run_etl_pipeline.py bronze   # Run only bronze ingestion
    python run_etl_pipeline.py silver   # Run only silver transformation
"""

import sys
import time
import traceback
from datetime import datetime


def run_bronze_ingestion():
    """Run the Bronze layer ingestion step."""
    from ingest_to_bronze import run_ingestion
    run_ingestion()


def run_silver_transformation():
    """Run the Silver layer transformation step."""
    from clean_transform_to_silver import run_silver_transformation as silver_transform
    silver_transform()


def run_pipeline(steps=None):
    """
    Execute the ETL pipeline.
    
    Args:
        steps: List of steps to run. Options: ['bronze', 'silver'].
               If None, runs all steps in order.
    """
    if steps is None:
        steps = ['bronze', 'silver']
    
    pipeline_start = time.time()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print("=" * 70)
    print("ETL PIPELINE ORCHESTRATOR")
    print(f"Started at: {timestamp}")
    print(f"Steps to run: {', '.join(steps)}")
    print("=" * 70)
    
    step_times = {}
    failed_step = None
    
    try:
        if 'bronze' in steps:
            print("\n" + "#" * 70)
            print("# STEP 1: BRONZE LAYER INGESTION")
            print("#" * 70 + "\n")
            
            step_start = time.time()
            run_bronze_ingestion()
            step_times['bronze'] = time.time() - step_start
            
            print(f"\n[OK] Bronze ingestion completed in {step_times['bronze']:.2f} seconds")
        
        if 'silver' in steps:
            print("\n" + "#" * 70)
            print("# STEP 2: SILVER LAYER TRANSFORMATION")
            print("#" * 70 + "\n")
            
            step_start = time.time()
            run_silver_transformation()
            step_times['silver'] = time.time() - step_start
            
            print(f"\n[OK] Silver transformation completed in {step_times['silver']:.2f} seconds")
    
    except Exception as e:
        failed_step = steps[len(step_times)]
        print(f"\n[ERROR] Pipeline failed at step: {failed_step}")
        print(f"Error: {str(e)}")
        traceback.print_exc()
    
    # Print summary
    pipeline_duration = time.time() - pipeline_start
    
    print("\n" + "=" * 70)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    
    for step, duration in step_times.items():
        status = "OK"
        print(f"  {step.upper():<20} {status:<10} {duration:.2f}s")
    
    if failed_step:
        print(f"  {failed_step.upper():<20} FAILED")
        print(f"\nPipeline FAILED after {pipeline_duration:.2f} seconds")
        return False
    else:
        print(f"\nPipeline completed successfully in {pipeline_duration:.2f} seconds")
        return True


def main():
    """Main entry point with CLI argument handling."""
    valid_steps = {'bronze', 'silver', 'all'}
    
    if len(sys.argv) > 1:
        requested = sys.argv[1].lower()
        
        if requested == '--help' or requested == '-h':
            print(__doc__)
            return
        
        if requested not in valid_steps:
            print(f"Error: Unknown step '{requested}'")
            print(f"Valid options: {', '.join(sorted(valid_steps))}")
            sys.exit(1)
        
        if requested == 'all':
            steps = None
        else:
            steps = [requested]
    else:
        steps = None  # Run all steps
    
    success = run_pipeline(steps)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
