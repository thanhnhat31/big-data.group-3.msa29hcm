"""
Unit and Integration Tests for Credit Transaction Fraud ETL Pipeline
"""

import pytest
import os
import sys

# Ensure root workspace directory is in sys.path for pytest
sys.path.insert(0, os.path.abspath("."))

from src.etl_module_credit_transaction import (
    check_local_file_integrity, 
    clean_and_transform_data
)


def test_credit_file_integrity_check():
    """
    Tests local file integrity verification function for credit transactions.
    """
    # Real mock directory should pass
    assert check_local_file_integrity("data/mock") is True
    
    # Check data/raw/credit_transaction_fraud if present
    if os.path.exists("data/raw/credit_transaction_fraud"):
        assert check_local_file_integrity("data/raw/credit_transaction_fraud") is True

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
            .appName("CreditTransactionETLTest") \
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


def test_clean_and_transform_credit_data_schema(spark):
    """
    Tests PySpark cleaning, type casting, date parsing, and feature engineering.
    Skips automatically if the `spark` fixture was skipped (no JVM environment).
    """
    if spark is None:
        pytest.skip("Spark fixture not available.")

    from pyspark.sql.types import DateType, TimestampType, LongType, DoubleType, IntegerType

    local_csv = os.path.abspath("data/mock/*.csv")

    df_cleaned = clean_and_transform_data(spark, local_csv)

    # 1. Assert DataFrame is not empty
    count = df_cleaned.count()
    assert count > 0, "Cleaned DataFrame should not be empty."

    # 2. Assert key schema columns exist
    expected_columns = {
        "TRANSACTION_ID", "TX_DATETIME", "CUSTOMER_ID", "TERMINAL_ID", "TX_AMOUNT",
        "TX_TIME_SECONDS", "TX_TIME_DAYS", "TX_FRAUD", "TX_FRAUD_SCENARIO",
        "tx_date", "tx_year_month", "tx_hour", "tx_day_of_week", "is_weekend",
        "is_night", "customer_tx_count", "customer_avg_amount", "terminal_tx_count"
    }
    actual_columns = set(df_cleaned.columns)
    missing_cols = expected_columns - actual_columns
    assert not missing_cols, f"Missing required columns in cleaned DataFrame: {missing_cols}"

    # 3. Assert Data Types
    schema_dict = {field.name: field.dataType for field in df_cleaned.schema.fields}
    assert isinstance(schema_dict["TRANSACTION_ID"], LongType), "TRANSACTION_ID should be LongType"
    assert isinstance(schema_dict["TX_DATETIME"], TimestampType), "TX_DATETIME should be TimestampType"
    assert isinstance(schema_dict["TX_AMOUNT"], DoubleType), "TX_AMOUNT should be DoubleType"
    assert isinstance(schema_dict["TX_TIME_SECONDS"], LongType), "TX_TIME_SECONDS should be LongType"
    assert isinstance(schema_dict["TX_TIME_DAYS"], IntegerType), "TX_TIME_DAYS should be IntegerType"
    assert isinstance(schema_dict["tx_date"], DateType), "tx_date should be DateType"
    assert isinstance(schema_dict["is_weekend"], IntegerType), "is_weekend should be IntegerType"
    assert isinstance(schema_dict["is_night"], IntegerType), "is_night should be IntegerType"

    # 4. Assert Null Handling
    null_tx_ids = df_cleaned.filter(df_cleaned.TRANSACTION_ID.isNull()).count()
    assert null_tx_ids == 0, "TRANSACTION_ID column should have no Null values."

    null_amounts = df_cleaned.filter(df_cleaned.TX_AMOUNT.isNull()).count()
    assert null_amounts == 0, "TX_AMOUNT column should have no Null values."

    null_time_secs = df_cleaned.filter(df_cleaned.TX_TIME_SECONDS.isNull()).count()
    assert null_time_secs == 0, "TX_TIME_SECONDS column should have no Null values."
