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


def configure_environment():
    """Set up Spark and Delta Lake environment variables."""
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

    print('OS:', os.name)
    print('JAVA_HOME:', os.environ.get('JAVA_HOME', '<not set>'))
    print('HADOOP_HOME:', os.environ.get('HADOOP_HOME', '<not set>'))


def create_spark_session():
    """Create a SparkSession with Delta Lake extensions enabled."""
    return SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.legacy.csv.headerCheck.enabled", "false") \
        .getOrCreate()


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


def merge_or_create_table(spark, df, path, dataset_name):
    """Perform MERGE or initial write for a dataset."""
    if DeltaTable.isDeltaTable(spark, path):
        print(f"{dataset_name}: Existing Delta table found. Merging new data...")
        target_table = DeltaTable.forPath(spark, path)
        target_table.alias("target").merge(
            df.alias("source"),
            "target.Row_id = source.Row_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        print(f"{dataset_name}: No existing Delta table found. Creating new table...")
        df.write.format("delta").mode("overwrite").save(path)


def load_to_duckdb(project_root, bronze_train_path, bronze_test_path, bronze_validation_path):
    """Load Delta tables into DuckDB."""
    with db.connect(os.path.join(project_root, "ProjectData.duckdb")) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        # Training data
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.reviews_train AS 
            SELECT * FROM delta_scan('{bronze_train_path}')
        """)
        train_count = con.execute("SELECT COUNT(*) FROM bronze.reviews_train").fetchone()[0]
        train_corrupt = con.execute("SELECT COUNT(*) FROM bronze.reviews_train WHERE _corrupt_record IS NOT NULL").fetchone()[0]
        print(f"Total rows training in db: {train_count}")
        print(f"Corrupt rows training in db: {train_corrupt}")

        # Test data
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.reviews_test AS 
            SELECT * FROM delta_scan('{bronze_test_path}')
        """)
        test_count = con.execute("SELECT COUNT(*) FROM bronze.reviews_test").fetchone()[0]
        test_corrupt = con.execute("SELECT COUNT(*) FROM bronze.reviews_test WHERE _corrupt_record IS NOT NULL").fetchone()[0]
        print(f"\nTotal rows test data in db: {test_count}")
        print(f"Corrupt rows test data in db: {test_corrupt}")

        # Validation data
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.reviews_validation AS 
            SELECT * FROM delta_scan('{bronze_validation_path}')
        """)
        validation_count = con.execute("SELECT COUNT(*) FROM bronze.reviews_validation").fetchone()[0]
        validation_corrupt = con.execute("SELECT COUNT(*) FROM bronze.reviews_validation WHERE _corrupt_record IS NOT NULL").fetchone()[0]
        print(f"\nTotal rows validation data in db: {validation_count}")
        print(f"Corrupt rows validation data in db: {validation_corrupt}")


def run_ingestion():
    """Main ingestion pipeline."""
    print("=" * 60)
    print("BRONZE LAYER INGESTION")
    print("=" * 60)

    # Configure environment
    configure_environment()

    # Initialize Spark
    spark = create_spark_session()

    try:
        # Get schemas
        transaction_schema_training, transaction_schema_test_val = get_transaction_schemas()

        # Set up paths
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, "../../"))

        raw_csv_dir = os.path.join(project_root, "reviews (copy)")
        bronze_train_path = os.path.join(project_root, "data", "bronze", "train")
        bronze_test_path = os.path.join(project_root, "data", "bronze", "test")
        bronze_validation_path = os.path.join(project_root, "data", "bronze", "validation")

        print(f"\nReading from: {raw_csv_dir}")
        print(f"Writing train to:      {bronze_train_path}")
        print(f"Writing test to:       {bronze_test_path}")
        print(f"Writing validation to: {bronze_validation_path}")

        # Clean up old non-Delta data if needed
        for path in [bronze_train_path, bronze_test_path, bronze_validation_path]:
            if os.path.exists(path) and not os.path.exists(os.path.join(path, "_delta_log")):
                shutil.rmtree(path)

        # Load training data
        print("\n--- Loading Training Data ---")
        base_df = load_csv_data(spark, raw_csv_dir, transaction_schema_training, "train-*.csv")
        train_count = base_df.count()
        print(f"Training data loaded: {train_count} rows")

        # Load test data
        print("\n--- Loading Test Data ---")
        test_df = load_csv_data(spark, raw_csv_dir, transaction_schema_test_val, "test_hidden.csv")
        test_count = test_df.count()
        print(f"Test data loaded: {test_count} rows")

        # Load validation data
        print("\n--- Loading Validation Data ---")
        validation_df = load_csv_data(spark, raw_csv_dir, transaction_schema_test_val, "validation_hidden.csv")
        validation_count = validation_df.count()
        print(f"Validation data loaded: {validation_count} rows")

        # Merge into Delta tables
        print("\n--- Writing to Delta Tables ---")
        merge_or_create_table(spark, base_df, bronze_train_path, "Training")
        merge_or_create_table(spark, test_df, bronze_test_path, "Test")
        merge_or_create_table(spark, validation_df, bronze_validation_path, "Validation")

        # Validate Delta tables
        print("\n--- Validating Delta Tables ---")
        train_table_df = spark.read.format("delta").load(bronze_train_path)
        test_table_df = spark.read.format("delta").load(bronze_test_path)
        validation_table_df = spark.read.format("delta").load(bronze_validation_path)

        print(f"Total rows training data: {train_table_df.count()}")
        print(f"Total rows test data: {test_table_df.count()}")
        print(f"Total rows validation data: {validation_table_df.count()}")

        # Load into DuckDB
        print("\n--- Loading into DuckDB ---")
        load_to_duckdb(project_root, bronze_train_path, bronze_test_path, bronze_validation_path)

        print("\n" + "=" * 60)
        print("BRONZE LAYER INGESTION COMPLETE")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_ingestion()
