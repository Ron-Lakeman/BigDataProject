"""
Ingest to Bronze Layer
CSV → PySpark → Delta Lake → DuckDB

This script ingests raw CSV files into the Bronze layer using PySpark and Delta Lake,
then loads them into DuckDB for downstream processing.

Handles three separate datasets:
- Training data (train-*.csv) → bronze.reviews_train
- Test data (test_hidden.csv) → bronze.reviews_test
- Validation data (validation_hidden.csv) → bronze.reviews_validation
"""

import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, monotonically_increasing_id, to_date
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
import duckdb as db
from prefect import task, flow
from prefect import task, get_run_logger
from prefect.artifacts import create_markdown_artifact


@task(name="Configure Spark Environment")
def configure_environment():

    # set up Spark and Delta Lake environment variables
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell'

    # Windows-only compatibility setup (safe no-op on macOS/Linux)
    if os.name == 'nt':
        if 'JAVA_HOME' not in os.environ or not os.environ['JAVA_HOME']:
            import glob
            jdk_candidates = sorted(glob.glob(r'C:\\Program Files\\Eclipse Adoptium\\jdk-*'), reverse=True)
            if jdk_candidates:
                os.environ['JAVA_HOME'] = jdk_candidates[0]

        if 'JAVA_HOME' in os.environ and os.environ['JAVA_HOME']:
            java_bin = os.path.join(os.environ['JAVA_HOME'], 'bin')
            if java_bin not in os.environ.get('PATH', ''):
                os.environ['PATH'] = java_bin + os.pathsep + os.environ.get('PATH', '')

        hadoop_home = os.path.abspath(os.path.join(os.getcwd(), '..', '..', '.tools', 'hadoop'))
        winutils_path = os.path.join(hadoop_home, 'bin', 'winutils.exe')
        if os.path.exists(winutils_path):
            os.environ['HADOOP_HOME'] = hadoop_home
            hadoop_bin = os.path.join(hadoop_home, 'bin')
            if hadoop_bin not in os.environ.get('PATH', ''):
                os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
    
    


@task(name="Create Spark Session", persist_result=False)
def create_spark_session():
    """Create a SparkSession with Delta Lake extensions enabled."""
    return SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.legacy.csv.headerCheck.enabled", "false") \
        .getOrCreate()
    


@task(name="Get Transaction Schemas")
def get_transaction_schemas():
    """Define the expected schemas for incoming CSV files."""
    transaction_schema_training = StructType([
        StructField("Row_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_parent", StringType(), True),
        StructField("product_title", StringType(), True),
        StructField("vine", StringType(), True),
        StructField("verified_purchase", StringType(), True),
        StructField("review_headline", StringType(), True),
        StructField("review_body", StringType(), True),
        StructField("review_date", StringType(), True),
        StructField("marketplace_id", StringType(), True),
        StructField("product_category_id", StringType(), True),
        StructField("label", StringType(), True),
        StructField("_corrupt_record", StringType(), True)
    ])

    transaction_schema_test_val = StructType([
        StructField("Row_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_parent", StringType(), True),
        StructField("product_title", StringType(), True),
        StructField("vine", StringType(), True),
        StructField("verified_purchase", StringType(), True),
        StructField("review_headline", StringType(), True),
        StructField("review_body", StringType(), True),
        StructField("review_date", StringType(), True),
        StructField("marketplace_id", StringType(), True),
        StructField("product_category_id", StringType(), True),
        StructField("_corrupt_record", StringType(), True)
    ])
    return transaction_schema_training, transaction_schema_test_val
    

@task(name="Load CSV Data to PySpark DataFrame", persist_result=False)
def load_csv_data(spark, raw_csv_dir, schema, path_glob_filter):
    """Load CSV data with metadata columns."""
    return (spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .option("pathGlobFilter", path_glob_filter)
        .csv(raw_csv_dir)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_load_date", to_date(current_timestamp()))
        .withColumn("_source_file", input_file_name())
        .withColumn("_index", monotonically_increasing_id())
    )

@task(name="Validate Data Counts", persist_result=False)
def validate_counts(dataset_name, old_count, new_count, final_count):
    """Verifies that the new CSV count matches the final Delta row count."""
    logger = get_run_logger()
    logger.info(f"[{dataset_name}] Old: {old_count} | New CSV: {new_count} | Final: {final_count}")

    if final_count != new_count:
        msg = f"[{dataset_name}] STRICT VALIDATION FAILED: New CSV length ({new_count}) does NOT equal Final Delta length ({final_count})"
        logger.error(msg)
        raise ValueError(msg)
    else:
        logger.info(f"[{dataset_name}] Strict Validation Passed: CSV data length perfectly matches Delta Table length")


@task(name="Get Existing Delta Table Count", persist_result=False)
def get_existing_table_count(spark, path):
    """Returns the row count of an existing Delta table or 0"""
    if DeltaTable.isDeltaTable(spark, path):
        return spark.read.format("delta").load(path).count()
    return 0


@task(name="Merge/Upload Delta Table", persist_result=False)
def merge_or_create_table(spark, df, path, dataset_name):
    """Perform MERGE or initial write for a dataset."""
    if DeltaTable.isDeltaTable(spark, path):
        target_table = DeltaTable.forPath(spark, path)
        target_table.alias("target").merge(
            df.alias("source"),
            "target.Row_id = source.Row_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        # if no existing Delta table found. Creating new table
        df.write.format("delta").mode("overwrite").save(path)


@task(name="Load Delta to DuckDB")
def load_to_duckdb(project_root, bronze_train_path, bronze_test_path, bronze_validation_path):
    """Load Delta tables into DuckDB."""
    with db.connect(os.path.join(project_root, "ProjectData.duckdb")) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        # Training data
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.reviews_train AS 
            SELECT * FROM delta_scan('{bronze_train_path}')
        """)
        # train_count = con.execute("SELECT COUNT(*) FROM bronze.reviews_train").fetchone()[0]
        train_corrupt = con.execute("SELECT COUNT(*) FROM bronze.reviews_train WHERE _corrupt_record IS NOT NULL").fetchone()[0]

        # Test data
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.reviews_test AS 
            SELECT * FROM delta_scan('{bronze_test_path}')
        """)
        # test_count = con.execute("SELECT COUNT(*) FROM bronze.reviews_test").fetchone()[0]
        test_corrupt = con.execute("SELECT COUNT(*) FROM bronze.reviews_test WHERE _corrupt_record IS NOT NULL").fetchone()[0]

        # Validation data
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.reviews_validation AS 
            SELECT * FROM delta_scan('{bronze_validation_path}')
        """)
        # validation_count = con.execute("SELECT COUNT(*) FROM bronze.reviews_validation").fetchone()[0]
        validation_corrupt = con.execute("SELECT COUNT(*) FROM bronze.reviews_validation WHERE _corrupt_record IS NOT NULL").fetchone()[0]
        return train_corrupt, test_corrupt, validation_corrupt

@flow(name="Ingest to Bronze Flow", log_prints=True)
def run_ingestion_bronze():
    """Main ingestion pipeline."""
    logger = get_run_logger()
    logger.info("Starting Bronze layer ingestion pipeline")

    # Configure environment
    configure_environment()
    logger.info("Spark environment configuration has finished.")

    # Initialize Spark
    spark = create_spark_session()
    logger.info("Spark session created")

    try:
        # Get schemas
        transaction_schema_training, transaction_schema_test_val = get_transaction_schemas()
        logger.info("Retrieved transaction schemas")


        # Set up paths
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, "../../"))
        raw_csv_dir = os.path.join(project_root, "reviews (copy)")
        bronze_train_path = os.path.join(project_root, "data", "bronze", "train")
        bronze_test_path = os.path.join(project_root, "data", "bronze", "test")
        bronze_validation_path = os.path.join(project_root, "data", "bronze", "validation")

        # clean up old non-Delta data if needed
        for path in [bronze_train_path, bronze_test_path, bronze_validation_path]:
            if os.path.exists(path) and not os.path.exists(os.path.join(path, "_delta_log")):
                logger.info(f"Removing invalid Delta path (missing _delta_log): {path}")
                shutil.rmtree(path)

        # Capture old counts BEFORE ingestion
        old_train_count = get_existing_table_count(spark, bronze_train_path)
        old_test_count = get_existing_table_count(spark, bronze_test_path)
        old_valid_count = get_existing_table_count(spark, bronze_validation_path)

        # training and test data is loaded from the source
        base_df = load_csv_data(spark, raw_csv_dir, transaction_schema_training, "train-*.csv")
        training_count = base_df.count()
        logger.info(f"Training data loaded with {training_count} rows")

        # load test data from source
        test_df = load_csv_data(spark, raw_csv_dir, transaction_schema_test_val, "test_hidden.csv")
        test_df_count = test_df.count()
        logger.info(f"Test data loaded with {test_df_count} rows")

        # load validation data
        validation_df = load_csv_data(spark, raw_csv_dir, transaction_schema_test_val, "validation_hidden.csv")
        validation_count = validation_df.count()
        logger.info(f"Validation data loaded with {validation_count} rows")

        # Merge into Delta tables
        merge_or_create_table(spark, base_df, bronze_train_path, "Training")
        merge_or_create_table(spark, test_df, bronze_test_path, "Test")
        merge_or_create_table(spark, validation_df, bronze_validation_path, "Validation")

        # Validate Delta tables
        train_table_df = spark.read.format("delta").load(bronze_train_path)
        test_table_df = spark.read.format("delta").load(bronze_test_path)
        validation_table_df = spark.read.format("delta").load(bronze_validation_path)

        final_train_count = train_table_df.count()
        final_test_count = test_table_df.count()
        final_valid_count = validation_table_df.count()

        logger.info(f"Total rows training data (post-merge): {final_train_count}")
        logger.info(f"Total rows test data (post-merge): {final_test_count}")
        logger.info(f"Total rows validation data (post-merge): {final_valid_count}")

        # Perform Data Count Validations
        validate_counts("Training", old_train_count, training_count, final_train_count)
        validate_counts("Test", old_test_count, test_df_count, final_test_count)
        validate_counts("Validation", old_valid_count, validation_count, final_valid_count)

        # Load into DuckDB
        train_corrupt, test_corrupt, validation_corrupt = load_to_duckdb(project_root, bronze_train_path, bronze_test_path, bronze_validation_path)
        
        create_markdown_artifact(
            key="ingestion-counts",
            markdown=f"""# Data Ingestion Summary
We tracked the dataset row counts across all stages of the ingestion process:

| Dataset | 1. Old (Pre-run) | 2. New (CSVs) | 3. Final (Delta) | 4. Corrupt Rows |
| :--- | :--- | :--- | :--- | :--- |
| **Training** | {old_train_count} | {training_count} | {final_train_count} | {train_corrupt} |
| **Test** | {old_test_count} | {test_df_count} | {final_test_count} | {test_corrupt} |
| **Validation**| {old_valid_count} | {validation_count} | {final_valid_count} | {validation_corrupt} |

*(Note: Data counts may fluctuate during merges as duplicate rows are updated.)*
""",
            description="Comprehensive row counts comparing pre-run, CSV, final Delta logic, and corrupt records."
        )
        
        
        
        print("BRONZE LAYER INGESTION DONE")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_ingestion_bronze()
