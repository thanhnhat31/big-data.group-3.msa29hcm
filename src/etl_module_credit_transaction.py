"""
ETL Module for Credit Transaction Fraud Big Data Pipeline
Handles:
1. Ingestion: File integrity checks & local filesystem to HDFS upload
2. Data Cleaning & Transformation: PySpark processing
   - Schema enforcement & casting (TX_DATETIME, TRANSACTION_ID, TX_AMOUNT, etc.)
   - Filtering invalid rows & null handling
   - Advanced feature engineering (datetime features, risk flags, monthly customer & terminal window aggregations)
3. Data Storage:
   - Save cleaned data as Intermediate Parquet files on HDFS
   - Register/Save into Hive tables in ORC/Parquet format partitioned by tx_year_month.
"""

import os
import sys
import glob
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, DoubleType, TimestampType, DateType
)
from pyspark.sql.window import Window


def get_spark_session(
    app_name: str = "CreditTransactionETL",
    hive_metastore_uri: Optional[str] = "thrift://hive-metastore:9083"
) -> SparkSession:
    """
    Initializes and returns a PySpark Session configured for HDFS and Hive Metastore.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "32") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.sql.orc.enabled", "true") \
        .config("spark.sql.parquet.writelegacyformat", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if hive_metastore_uri:
        builder = builder \
            .config("hive.metastore.uris", hive_metastore_uri) \
            .enableHiveSupport()

    return builder.getOrCreate()


def check_local_file_integrity(local_dir: str) -> bool:
    """
    Verifies that local directory exists and contains valid non-empty CSV files.
    """
    if not os.path.exists(local_dir):
        print(f"[ERROR] Directory '{local_dir}' does not exist.")
        return False

    csv_files = glob.glob(os.path.join(local_dir, "*.csv"))

    if not csv_files:
        print(f"[ERROR] Directory '{local_dir}' must contain CSV files.")
        return False

    # Check for empty (0-byte) files
    for file_path in csv_files:
        if os.path.getsize(file_path) == 0:
            print(f"[WARNING] File '{file_path}' is empty (0 bytes).")
            return False

    print(f"[INTEGRITY CHECK] Passed: Found {len(csv_files)} CSVs in '{local_dir}'.")
    return True


def ingest_local_to_hdfs(spark: SparkSession, local_dir: str, hdfs_dir: str) -> bool:
    """
    Validates local files and imports raw CSV files from local path to HDFS 
    using Spark's Java Hadoop FileSystem API (works seamlessly across host & Docker).
    """
    if not check_local_file_integrity(local_dir):
        raise ValueError(f"Integrity check failed for directory: {local_dir}")

    print(f"[INGESTION] Uploading files from '{local_dir}' to HDFS path '{hdfs_dir}'...")

    try:
        sc = spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path

        abs_local_dir = os.path.abspath(local_dir)
        csv_files = glob.glob(os.path.join(abs_local_dir, "*.csv"))

        hdfs_target_uri = hdfs_dir if hdfs_dir.startswith("hdfs://") else f"hdfs://namenode:9000{hdfs_dir}"
        hdfs_path = Path(hdfs_target_uri)
        hdfs_fs = hdfs_path.getFileSystem(conf)

        hdfs_csv_dir = Path(f"{hdfs_target_uri}/csv")
        hdfs_fs.mkdirs(hdfs_csv_dir)

        for f in csv_files:
            src = Path(os.path.abspath(f))
            dst = Path(f"{hdfs_target_uri}/csv/{os.path.basename(f)}")
            hdfs_fs.copyFromLocalFile(False, True, src, dst)

        print(f"[INGESTION] Successfully uploaded {len(csv_files)} CSVs to HDFS.")
        return True
    except Exception as e:
        print(f"[INGESTION WARNING] JVM Hadoop upload failed: {e}")
        raise e


def clean_and_transform_data(spark: SparkSession, hdfs_csv_path):
    """
    Reads CSV raw credit transaction data from HDFS or local filesystem, performs cleaning,
    casting, null handling, datetime feature extraction, and window aggregations.
    """
    # Expand wildcard paths locally when not reading from HDFS
    if isinstance(hdfs_csv_path, str) and "*" in hdfs_csv_path and not hdfs_csv_path.startswith("hdfs://"):
        matched_csv = glob.glob(hdfs_csv_path)
        if matched_csv:
            hdfs_csv_path = matched_csv

    print("[CLEANING] Step 1: Loading raw CSV transaction data...")
    df_raw = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(hdfs_csv_path)

    print("[CLEANING] Step 2: Filtering invalid & missing rows...")
    df_clean = df_raw.filter(F.col("TRANSACTION_ID").isNotNull() & (F.col("TRANSACTION_ID") != "#VALUE!"))

    print("[CLEANING] Step 3: Data Type Casting & Datetime Parsing...")
    df_clean = df_clean \
        .withColumn("TRANSACTION_ID", F.col("TRANSACTION_ID").cast(LongType())) \
        .withColumn("TX_DATETIME", F.to_timestamp(F.col("TX_DATETIME"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("CUSTOMER_ID", F.col("CUSTOMER_ID").cast(LongType())) \
        .withColumn("TERMINAL_ID", F.col("TERMINAL_ID").cast(LongType())) \
        .withColumn("TX_AMOUNT", F.col("TX_AMOUNT").cast(DoubleType())) \
        .withColumn("TX_TIME_SECONDS", F.col("TX_TIME_SECONDS").cast(LongType())) \
        .withColumn("TX_TIME_DAYS", F.col("TX_TIME_DAYS").cast(IntegerType())) \
        .withColumn("TX_FRAUD", F.col("TX_FRAUD").cast(IntegerType())) \
        .withColumn("TX_FRAUD_SCENARIO", F.col("TX_FRAUD_SCENARIO").cast(IntegerType())) \
        .withColumn("tx_date", F.to_date(F.col("TX_DATETIME"))) \
        .withColumn("tx_year_month", F.date_format(F.col("TX_DATETIME"), "yyyy-MM")) \
        .withColumn("tx_hour", F.hour(F.col("TX_DATETIME"))) \
        .withColumn("tx_day_of_week", F.date_format(F.col("TX_DATETIME"), "E"))

    # Fill default numeric values after cast using F.coalesce
    df_clean = df_clean \
        .withColumn("TX_AMOUNT", F.coalesce(F.col("TX_AMOUNT"), F.lit(0.0))) \
        .withColumn("TX_TIME_SECONDS", F.coalesce(F.col("TX_TIME_SECONDS"), F.lit(0).cast(LongType()))) \
        .withColumn("TX_TIME_DAYS", F.coalesce(F.col("TX_TIME_DAYS"), F.lit(0).cast(IntegerType()))) \
        .withColumn("TX_FRAUD", F.coalesce(F.col("TX_FRAUD"), F.lit(0).cast(IntegerType()))) \
        .withColumn("TX_FRAUD_SCENARIO", F.coalesce(F.col("TX_FRAUD_SCENARIO"), F.lit(0).cast(IntegerType())))

    print("[CLEANING] Step 4: Advanced Feature Engineering & Window Aggregations...")
    df_features = df_clean \
        .withColumn("is_weekend", F.when(F.col("tx_day_of_week").isin("Sat", "Sun"), 1).otherwise(0)) \
        .withColumn("is_night", F.when((F.col("tx_hour") >= 0) & (F.col("tx_hour") < 6), 1).otherwise(0))

    # Scoping Window partitions to (tx_year_month, CUSTOMER_ID) and (tx_year_month, TERMINAL_ID)
    # keeps window aggregations local to each partition, preventing massive cross-month shuffles
    window_customer = Window.partitionBy("tx_year_month", "CUSTOMER_ID")
    window_terminal = Window.partitionBy("tx_year_month", "TERMINAL_ID")

    df_features = df_features \
        .withColumn("customer_tx_count", F.count("TRANSACTION_ID").over(window_customer)) \
        .withColumn("customer_avg_amount", F.avg("TX_AMOUNT").over(window_customer)) \
        .withColumn("terminal_tx_count", F.count("TRANSACTION_ID").over(window_terminal))

    return df_features


def save_intermediate_parquet(
    df, 
    hdfs_parquet_dir: str = "hdfs://namenode:9000/credit_transaction/processed/parquet"
):
    """
    Saves cleaned DataFrame to Intermediate Parquet format on HDFS partitioned by tx_year_month.
    """
    print(f"[INTERMEDIATE STORAGE] Saving intermediate Parquet data to '{hdfs_parquet_dir}'...")
    df_partitioned = df.repartition("tx_year_month")
    
    df_partitioned.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("tx_year_month") \
        .save(hdfs_parquet_dir)
    print("[INTERMEDIATE STORAGE] Intermediate Parquet successfully saved.")


def save_to_hive(
    df, 
    database_name: str = "credit_transaction_db", 
    table_name: str = "cleaned_transactions",
    parquet_path: Optional[str] = None
):
    """
    Saves/registers the cleaned DataFrame into Hive Warehouse table.
    """
    spark = df.sparkSession
    print(f"[STORAGE] Saving cleaned data to Hive table `{database_name}.{table_name}`...")
    
    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    df_partitioned = df.repartition("tx_year_month")

    if parquet_path:
        print(f"[STORAGE] Registering Hive external table pointing to Parquet: {parquet_path}")
        spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
        
        df_partitioned.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("path", parquet_path) \
            .option("compression", "snappy") \
            .partitionBy("tx_year_month") \
            .saveAsTable(f"{database_name}.{table_name}")
    else:
        spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
        df_partitioned.write \
            .mode("overwrite") \
            .format("orc") \
            .option("compression", "snappy") \
            .partitionBy("tx_year_month") \
            .saveAsTable(f"{database_name}.{table_name}")

    print(f"[STORAGE] Data successfully written to Hive table `{database_name}.{table_name}`.")


def run_etl_pipeline(
    local_dir: str = "data/mock", 
    use_hdfs: bool = True,
    save_parquet: bool = True
):
    """
    Main ETL orchestration function.
    """
    print(f"=== STARTING E2E CREDIT TRANSACTION ETL PIPELINE (Mode: {'MOCK' if 'mock' in local_dir else 'RAW'}) ===")
    
    hdfs_raw_base = "hdfs://namenode:9000/credit_transaction/raw"
    hdfs_parquet_dir = "hdfs://namenode:9000/credit_transaction/processed/parquet"

    # Step 1: Initialize Spark Session
    spark = get_spark_session()

    # Step 2: Ingestion
    if use_hdfs:
        ingest_local_to_hdfs(spark, local_dir, hdfs_raw_base)
        hdfs_csv = f"{hdfs_raw_base}/csv/*.csv"
    else:
        hdfs_csv = f"{local_dir}/*.csv"

    # Step 3: Spark Data Cleaning & Transformation
    df_cleaned = clean_and_transform_data(spark, hdfs_csv)

    # Step 4: Intermediate Parquet Storage
    if save_parquet and use_hdfs:
        save_intermediate_parquet(df_cleaned, hdfs_parquet_dir)

    # Step 5: Storage in Hive
    save_to_hive(df_cleaned, parquet_path=hdfs_parquet_dir if save_parquet else None)

    print("=== E2E CREDIT TRANSACTION ETL PIPELINE COMPLETED SUCCESSFULLY ===")
    return df_cleaned


if __name__ == "__main__":
    # Default execution uses mock data first as requested by user
    target_dir = sys.argv[1] if len(sys.argv) > 1 else "data/mock"
    run_etl_pipeline(local_dir=target_dir)
