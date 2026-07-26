"""
Unit and Integration Tests for YouTube Trending ETL Pipeline
"""

import pytest
import os
import sys

# Ensure root workspace directory is in sys.path for pytest
sys.path.insert(0, os.path.abspath("."))

from src.etl_module import check_local_file_integrity, clean_and_transform_data


def test_file_integrity_check():
    """
    Tests local file integrity verification function.
    """
    # Real mock directory should pass
    assert check_local_file_integrity("data/mock") is True
    
    # Check data/raw/trending_youtube if present
    if os.path.exists("data/raw/trending_youtube"):
        assert check_local_file_integrity("data/raw/trending_youtube") is True

    # Non-existent directory should fail
    assert check_local_file_integrity("data/invalid_directory_path") is False


@pytest.fixture(scope="module")
def spark():
    """
    Fixture for creating a PySpark session for testing.
    Skips test if Java/PySpark environment is not configured locally.
    """
    import shutil
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        java_home_clean = java_home.rstrip("/\\")
        if java_home_clean.endswith("bin"):
            java_home_clean = os.path.dirname(java_home_clean)
            os.environ["JAVA_HOME"] = java_home_clean
            java_home = java_home_clean

    if java_home and not os.path.exists(java_home):
        pytest.skip(f"JAVA_HOME path is invalid or does not exist: '{java_home}'")
    if not shutil.which("java") and not (java_home and os.path.exists(os.path.join(java_home, "bin", "java.exe"))):
        pytest.skip("Java (JDK) environment not found on PATH or JAVA_HOME.")

    try:
        from pyspark.sql import SparkSession
        session = SparkSession.builder \
            .appName("ETLSchemaTest") \
            .master("local[1]") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()
    except Exception as e:
        pytest.skip(f"PySpark JVM environment not available locally: {e}")

    try:
        yield session
    finally:
        try:
            session.stop()
        except Exception:
            pass


def test_clean_and_transform_data_schema(spark):
    """
    Tests PySpark cleaning, category mapping, type casting, and feature engineering.
    """
    from pyspark.sql.types import DateType, TimestampType, LongType, DoubleType, ArrayType

    local_csv = os.path.abspath("data/mock/*.csv")
    local_json = os.path.abspath("data/mock/*.json")

    df_cleaned = clean_and_transform_data(spark, local_csv, local_json)

    # 1. Assert DataFrame is not empty
    count = df_cleaned.count()
    assert count > 0, "Cleaned DataFrame should not be empty."

    # 2. Assert key schema columns exist
    expected_columns = {
        "video_id", "trending_date", "title", "channel_title", "category_id",
        "category_title", "publish_time", "publish_date", "publish_hour",
        "publish_day_of_week", "tags", "tags_array", "views", "likes", "dislikes",
        "comment_count", "comments_disabled", "ratings_disabled",
        "video_error_or_removed", "description", "days_to_trend",
        "engagement_rate", "like_ratio", "total_days_trending", "country",
        "trending_year_month"
    }
    actual_columns = set(df_cleaned.columns)
    missing_cols = expected_columns - actual_columns
    assert not missing_cols, f"Missing required columns in cleaned DataFrame: {missing_cols}"

    # 3. Assert Data Types
    schema_dict = {field.name: field.dataType for field in df_cleaned.schema.fields}
    assert isinstance(schema_dict["trending_date"], DateType), "trending_date should be DateType"
    assert isinstance(schema_dict["publish_time"], TimestampType), "publish_time should be TimestampType"
    assert isinstance(schema_dict["views"], LongType), "views should be LongType"
    assert isinstance(schema_dict["engagement_rate"], DoubleType), "engagement_rate should be DoubleType"
    assert isinstance(schema_dict["tags_array"], ArrayType), "tags_array should be ArrayType"

    # 4. Assert Null Handling
    null_descriptions = df_cleaned.filter(df_cleaned.description.isNull()).count()
    assert null_descriptions == 0, "Description column should have no Null values."

    null_countries = df_cleaned.filter(df_cleaned.country.isNull()).count()
    assert null_countries == 0, "Country column should have no Null values."


def test_derived_metrics_correctness(spark):
    """
    Tests derived metrics logic (engagement rate, like ratio, total days trending).
    """
    local_csv = os.path.abspath("data/mock/*.csv")
    local_json = os.path.abspath("data/mock/*.json")

    df_cleaned = clean_and_transform_data(spark, local_csv, local_json)
    row = df_cleaned.filter(df_cleaned.views > 0).first()

    if row:
        calc_engagement = (row.likes + row.dislikes + row.comment_count) / row.views
        assert pytest.approx(row.engagement_rate, 0.0001) == calc_engagement, "Engagement rate calculation mismatch."

        if (row.likes + row.dislikes) > 0:
            calc_like_ratio = row.likes / (row.likes + row.dislikes)
            assert pytest.approx(row.like_ratio, 0.0001) == calc_like_ratio, "Like ratio calculation mismatch."

        assert row.total_days_trending >= 1, "Total days trending should be >= 1"
