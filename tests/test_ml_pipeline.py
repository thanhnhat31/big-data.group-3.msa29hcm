"""
Unit tests for Phase 4 Spark MLlib fraud detection pipeline.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath("."))

from src.ml_module import (
    FEATURE_COLS,
    LABEL_COL,
    WEIGHT_COL,
    add_class_weights,
    build_ml_pipeline,
    evaluate_predictions,
    prepare_feature_frame,
    train_and_evaluate,
)


@pytest.fixture(scope="module")
def spark():
    """Local Spark session for ML unit tests; skip if JVM/PySpark unavailable."""
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
    if not shutil.which("java") and not (
        java_home and os.path.exists(os.path.join(java_home, "bin", "java.exe"))
    ):
        pytest.skip("Java (JDK) environment not found on PATH or JAVA_HOME.")

    try:
        from pyspark.sql import SparkSession

        session = (
            SparkSession.builder.appName("CreditTransactionMLTest")
            .master("local[1]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
    except Exception as exc:
        pytest.skip(f"PySpark JVM environment not available locally: {exc}")

    try:
        yield session
    finally:
        try:
            session.stop()
        except Exception:
            pass


def _synthetic_transactions(spark):
    """
    Tiny imbalanced dataset with both classes (mock CSV has no fraud labels).
    Columns match Phase 4 FEATURE_COLS + TX_FRAUD.
    """
    rows = []
    # 20 legitimate transactions
    for i in range(20):
        rows.append(
            (
                20.0 + i,
                float(1000 + i),
                i % 24,
                1 if i % 7 == 0 else 0,
                1 if i % 24 < 6 else 0,
                10 + (i % 5),
                40.0 + i,
                30 + (i % 3),
                0,
            )
        )
    # 5 fraud transactions (night + higher amount pattern)
    for i in range(5):
        rows.append(
            (
                250.0 + i * 10,
                float(5000 + i),
                i % 5,
                1,
                1,
                3 + i,
                80.0 + i,
                120 + i,
                1,
            )
        )

    return spark.createDataFrame(rows, FEATURE_COLS + [LABEL_COL])


def test_feature_column_contract():
    """Phase 4 assignment feature list must stay exact."""
    assert FEATURE_COLS == [
        "TX_AMOUNT",
        "TX_TIME_SECONDS",
        "tx_hour",
        "is_weekend",
        "is_night",
        "customer_tx_count",
        "customer_avg_amount",
        "terminal_tx_count",
    ]


def test_prepare_feature_frame_and_class_weights(spark):
    df = _synthetic_transactions(spark)
    prepared = prepare_feature_frame(df)
    assert set(FEATURE_COLS + [LABEL_COL]).issubset(set(prepared.columns))

    weighted = add_class_weights(prepared)
    assert WEIGHT_COL in weighted.columns

    weights = {
        float(row[LABEL_COL]): float(row[WEIGHT_COL])
        for row in weighted.select(LABEL_COL, WEIGHT_COL).distinct().collect()
    }
    # Minority fraud class must receive higher weight
    assert weights[1.0] > weights[0.0]


def test_build_ml_pipeline_stages():
    pipeline = build_ml_pipeline()
    stage_names = [stage.__class__.__name__ for stage in pipeline.getStages()]
    assert stage_names == ["VectorAssembler", "StandardScaler", "LogisticRegression"]

    logistic = pipeline.getStages()[-1]
    assert logistic.getWeightCol() == WEIGHT_COL
    assert logistic.getLabelCol() == LABEL_COL
    assert logistic.getFeaturesCol() == "features"


def test_train_and_evaluate_pr_and_roc(spark):
    df = _synthetic_transactions(spark)
    model, metrics, train_df, predictions = train_and_evaluate(df, train_ratio=0.7, seed=42)

    assert model is not None
    assert train_df.count() > 0
    assert predictions.count() > 0
    assert "areaUnderPR" in metrics and "areaUnderROC" in metrics
    assert 0.0 <= metrics["areaUnderPR"] <= 1.0
    assert 0.0 <= metrics["areaUnderROC"] <= 1.0

    # Evaluator path used by Phase 4.5
    again = evaluate_predictions(predictions)
    assert set(again.keys()) == {"areaUnderPR", "areaUnderROC"}


def test_add_class_weights_requires_both_classes(spark):
    only_legit = _synthetic_transactions(spark).filter(f"{LABEL_COL} = 0")
    prepared = prepare_feature_frame(only_legit)
    with pytest.raises(ValueError, match="Need both classes"):
        add_class_weights(prepared)
