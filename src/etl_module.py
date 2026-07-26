"""
ETL Module for YouTube Trending Big Data Pipeline
Handles:
1. Ingestion: File integrity checks & local filesystem to HDFS upload
2. Data Cleaning & Transformation: PySpark processing
   - Schema enforcement & casting (trending_date, publish_time)
   - Category mapping from JSON files
   - Null handling & data standardization
   - Advanced feature engineering (engagement rate, time-to-trend, tag arrays, days trending)
3. Data Storage: 
   - Save cleaned data as Intermediate Parquet files on HDFS
   - Register/Save into Hive tables in ORC/Parquet format partitioned by country and trending_year_month.
"""

import os
import sys
import glob
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, BooleanType, DateType, TimestampType, ArrayType
)
from pyspark.sql.window import Window


def get_spark_session(
    app_name: str = "YouTubeTrendingETL", 
    hive_metastore_uri: Optional[str] = "thrift://hive-metastore:9083"
) -> SparkSession:
    """
    Initializes and returns a PySpark Session configured for HDFS and Hive Metastore.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
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
    Verifies that local raw/mock directory exists and contains valid non-empty CSV and JSON files.
    """
    if not os.path.exists(local_dir):
        print(f"[ERROR] Directory '{local_dir}' does not exist.")
        return False

    csv_files = glob.glob(os.path.join(local_dir, "*.csv"))
    json_files = glob.glob(os.path.join(local_dir, "*.json"))

    if not csv_files or not json_files:
        print(f"[ERROR] Directory '{local_dir}' must contain both CSV and JSON files.")
        return False

    # Check for empty (0-byte) files
    for file_path in csv_files + json_files:
        if os.path.getsize(file_path) == 0:
            print(f"[WARNING] File '{file_path}' is empty (0 bytes).")
            return False

    print(f"[INTEGRITY CHECK] Passed: Found {len(csv_files)} CSVs and {len(json_files)} JSONs in '{local_dir}'.")
    return True


def ingest_local_to_hdfs(spark: SparkSession, local_dir: str, hdfs_dir: str) -> bool:
    """
    Validates local files and imports raw files (CSV, JSON) from local path to HDFS 
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
        json_files = glob.glob(os.path.join(abs_local_dir, "*.json"))

        hdfs_target_uri = hdfs_dir if hdfs_dir.startswith("hdfs://") else f"hdfs://namenode:9000{hdfs_dir}"
        hdfs_path = Path(hdfs_target_uri)
        hdfs_fs = hdfs_path.getFileSystem(conf)

        hdfs_csv_dir = Path(f"{hdfs_target_uri}/csv")
        hdfs_json_dir = Path(f"{hdfs_target_uri}/json")

        hdfs_fs.mkdirs(hdfs_csv_dir)
        hdfs_fs.mkdirs(hdfs_json_dir)

        for f in csv_files:
            src = Path(os.path.abspath(f))
            dst = Path(f"{hdfs_target_uri}/csv/{os.path.basename(f)}")
            hdfs_fs.copyFromLocalFile(False, True, src, dst)

        for f in json_files:
            src = Path(os.path.abspath(f))
            dst = Path(f"{hdfs_target_uri}/json/{os.path.basename(f)}")
            hdfs_fs.copyFromLocalFile(False, True, src, dst)

        print(f"[INGESTION] Successfully uploaded {len(csv_files)} CSVs and {len(json_files)} JSONs to HDFS.")
        return True
    except Exception as e:
        print(f"[INGESTION WARNING] JVM Hadoop upload failed: {e}")
        raise e


def clean_and_transform_data(spark: SparkSession, hdfs_csv_path: str, hdfs_json_path: str):
    """
    Reads CSV and JSON raw data from HDFS, performs cleaning, category mapping,
    casting, null handling, and feature engineering.
    """
    print("[CLEANING] Step 1: Loading raw CSV data...")
    # Read raw CSVs with header and multiline support
    df_raw = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(hdfs_csv_path)

    # Add country column extracted from input file path (e.g. USvideos.csv -> US)
    # Regex anchored to uppercase 2-letter prefix immediately before 'videos.' to avoid false matches
    df_raw = df_raw.withColumn(
        "country",
        F.upper(F.regexp_extract(F.input_file_name(), r"/([A-Za-z]{2})videos\.", 1))
    )

    print("[CLEANING] Step 2: Parsing JSON category files...")
    # Read JSON category mappings
    df_json_raw = spark.read \
        .option("multiLine", "true") \
        .json(hdfs_json_path)

    # Extract country code from JSON filename (e.g. US_category_id.json -> US)
    df_json_raw = df_json_raw.withColumn(
        "country",
        F.upper(F.regexp_extract(F.input_file_name(), r"([A-Za-z]{2})_category_id", 1))
    )

    # Explode items array in JSON to extract category_id and category_title
    df_categories = df_json_raw \
        .select("country", F.explode("items").alias("item")) \
        .select(
            F.col("country"),
            F.col("item.id").cast("integer").alias("category_id"),
            F.col("item.snippet.title").alias("category_title")
        ) \
        .dropDuplicates(["country", "category_id"])

    print("[CLEANING] Step 3: Handling missing values & filtering invalid rows...")
    # Filter out missing video_id or rogue values
    df_clean = df_raw.filter(F.col("video_id").isNotNull() & (F.col("video_id") != "#VALUE!"))

    # Fill default string values for Nulls (before casting - columns are still StringType here)
    df_clean = df_clean.fillna({
        "title": "Untitled",
        "channel_title": "Unknown Channel",
        "tags": "[none]",
        "description": "No description"
    })

    print("[CLEANING] Step 4: Data Type Casting & Datetime Parsing...")
    # Cast Data Types & Parse Dates
    df_clean = df_clean \
        .withColumn("category_id", F.col("category_id").cast(IntegerType())) \
        .withColumn("views", F.col("views").cast(LongType())) \
        .withColumn("likes", F.col("likes").cast(LongType())) \
        .withColumn("dislikes", F.col("dislikes").cast(LongType())) \
        .withColumn("comment_count", F.col("comment_count").cast(LongType())) \
        .withColumn("comments_disabled", F.col("comments_disabled").cast(BooleanType())) \
        .withColumn("ratings_disabled", F.col("ratings_disabled").cast(BooleanType())) \
        .withColumn("video_error_or_removed", F.col("video_error_or_removed").cast(BooleanType())) \
        .withColumn("trending_date", F.to_date(F.col("trending_date"), "yy.dd.MM")) \
        .withColumn("publish_time", F.to_timestamp(F.col("publish_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
        .withColumn("publish_date", F.to_date(F.col("publish_time"))) \
        .withColumn("publish_hour", F.hour(F.col("publish_time"))) \
        .withColumn("publish_day_of_week", F.date_format(F.col("publish_time"), "E"))

    # Fill default numeric/boolean values AFTER cast (cast may produce Null from bad strings)
    df_clean = df_clean.fillna({"views": 0, "likes": 0, "dislikes": 0, "comment_count": 0})
    df_clean = df_clean.fillna({"comments_disabled": False, "ratings_disabled": False, "video_error_or_removed": False})

    print("[CLEANING] Step 5: Joining with Category Mapping...")
    # Join with categories
    df_mapped = df_clean.join(
        df_categories,
        on=["country", "category_id"],
        how="left"
    ).fillna({"category_title": "Non-Active / Unknown Category"})

    print("[CLEANING] Step 6: Advanced Feature Engineering & Aggregations...")
    # Feature Engineering
    # 1. Days taken from publish to trending
    df_features = df_mapped.withColumn(
        "days_to_trend",
        F.datediff(F.col("trending_date"), F.col("publish_date"))
    )

    # 2. Total engagement rate & like ratio
    df_features = df_features \
        .withColumn(
            "engagement_rate",
            F.when(F.col("views") > 0, (F.col("likes") + F.col("dislikes") + F.col("comment_count")) / F.col("views"))
            .otherwise(0.0)
        ) \
        .withColumn(
            "like_ratio",
            F.when((F.col("likes") + F.col("dislikes")) > 0, F.col("likes") / (F.col("likes") + F.col("dislikes")))
            .otherwise(0.0)
        )

    # 3. Tags Array for downstream NLP/Graph analysis
    df_features = df_features.withColumn(
        "tags_array",
        F.split(F.regexp_replace(F.col("tags"), '"', ''), r"\|")
    )

    # 4. Partition Helper: Year-Month of trending date
    df_features = df_features.withColumn(
        "trending_year_month",
        F.date_format(F.col("trending_date"), "yyyy-MM")
    )

    # 5. Windowing: Calculate total days video stayed on trending list per country
    window_video = Window.partitionBy("country", "video_id")
    df_features = df_features.withColumn(
        "total_days_trending",
        F.count("trending_date").over(window_video)
    )

    return df_features


def save_intermediate_parquet(df, hdfs_parquet_dir: str = "hdfs://namenode:9000/youtube/processed/parquet"):
    """
    Saves cleaned DataFrame to Intermediate Parquet format on HDFS partitioned by country and trending_year_month.
    """
    print(f"[INTERMEDIATE STORAGE] Saving intermediate Parquet data to '{hdfs_parquet_dir}'...")
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("country", "trending_year_month") \
        .save(hdfs_parquet_dir)
    print(f"[INTERMEDIATE STORAGE] Intermediate Parquet successfully saved.")


def save_to_hive(
    df, 
    database_name: str = "youtube_db", 
    table_name: str = "cleaned_videos",
    parquet_path: Optional[str] = None
):
    """
    Saves/registers the cleaned DataFrame into Hive Warehouse table.
    """
    print(f"[STORAGE] Saving cleaned data to Hive table `{database_name}.{table_name}`...")
    
    # Create database if not exists
    df.sparkSession.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    if parquet_path:
        print(f"[STORAGE] Registering Hive external table pointing to Parquet: {parquet_path}")
        # Drop existing catalog table entry to avoid LOCATION_ALREADY_EXISTS conflict
        df.sparkSession.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
        
        # saveAsTable with explicit path registers Hive External Table and auto-recovers partitions
        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("path", parquet_path) \
            .option("compression", "snappy") \
            .partitionBy("country", "trending_year_month") \
            .saveAsTable(f"{database_name}.{table_name}")
    else:
        df.sparkSession.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
        df.write \
            .mode("overwrite") \
            .format("orc") \
            .option("compression", "snappy") \
            .partitionBy("country", "trending_year_month") \
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
    print(f"=== STARTING E2E ETL PIPELINE (Mode: {'MOCK' if 'mock' in local_dir else 'RAW'}) ===")
    
    hdfs_raw_base = "hdfs://namenode:9000/youtube/raw"
    hdfs_parquet_dir = "hdfs://namenode:9000/youtube/processed/parquet"

    # Step 1: Initialize Spark Session
    spark = get_spark_session()

    # Step 2: Ingestion
    if use_hdfs:
        ingest_local_to_hdfs(spark, local_dir, hdfs_raw_base)
        hdfs_csv = f"{hdfs_raw_base}/csv/*.csv"
        hdfs_json = f"{hdfs_raw_base}/json/*.json"
    else:
        hdfs_csv = f"{local_dir}/*.csv"
        hdfs_json = f"{local_dir}/*.json"

    # Step 3: Spark Data Cleaning & Transformation
    df_cleaned = clean_and_transform_data(spark, hdfs_csv, hdfs_json)

    # Step 4: Intermediate Parquet Storage
    if save_parquet and use_hdfs:
        save_intermediate_parquet(df_cleaned, hdfs_parquet_dir)

    # Step 5: Storage in Hive
    save_to_hive(df_cleaned, parquet_path=hdfs_parquet_dir if save_parquet else None)

    print("=== E2E ETL PIPELINE COMPLETED SUCCESSFULLY ===")
    return df_cleaned


if __name__ == "__main__":
    # Default execution uses mock data first as requested by user
    target_dir = sys.argv[1] if len(sys.argv) > 1 else "data/mock"
    run_etl_pipeline(local_dir=target_dir)
