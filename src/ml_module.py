"""
Phase 4: Spark MLlib pipeline for Credit Card Transaction Fraud Detection.

Implements:
1. Load cleaned transactions from Hive (CSV/mock fallback)
2. VectorAssembler feature packing
3. Class weighting for imbalanced TX_FRAUD labels
4. Logistic Regression training
5. Evaluation via Area Under PR-Curve and ROC-AUC (not Accuracy)
"""

from __future__ import annotations

import os
import sys
from typing import Dict, List, Optional, Tuple

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Feature columns required by Phase 4 assignment (exclude ID columns & TX_FRAUD_SCENARIO)
FEATURE_COLS: List[str] = [
    "TX_AMOUNT",
    "TX_TIME_SECONDS",
    "tx_hour",
    "is_weekend",
    "is_night",
    "customer_tx_count",
    "customer_avg_amount",
    "terminal_tx_count",
]

LABEL_COL = "TX_FRAUD"
WEIGHT_COL = "class_weight"
ASSEMBLED_COL = "assembled_features"
FEATURES_COL = "features"

HIVE_TABLE = "credit_transaction_db.cleaned_transactions"


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def get_ml_spark_session(
    app_name: str = "CreditTransactionML",
    hive_metastore_uri: Optional[str] = "thrift://hive-metastore:9083",
) -> SparkSession:
    """
    Create a Spark session with Hive support when available; otherwise local[*].
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
        .config("spark.driver.memory", "4g")
    )

    if hive_metastore_uri:
        builder = (
            builder.config("hive.metastore.uris", hive_metastore_uri)
            .enableHiveSupport()
        )

    spark = builder.getOrCreate()
    try:
        if hive_metastore_uri:
            spark.sql("SHOW DATABASES")
        print(f"[ML] Spark session ready (Hive URI: {hive_metastore_uri}).")
        return spark
    except Exception as exc:
        print(f"[ML] Hive session unavailable ({exc}); switching to local Spark session.")
        try:
            spark.stop()
        except Exception:
            pass
        return (
            SparkSession.builder.appName(f"{app_name}_Local")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )


def load_ml_dataset(
    spark: SparkSession,
    hive_table: str = HIVE_TABLE,
    fallback_dirs: Optional[List[str]] = None,
) -> Tuple[DataFrame, str]:
    """
    Load cleaned transactions for ML.

    Prefer Hive table; fall back to local CSV dirs via ETL clean_and_transform_data.
    Returns (dataframe, source_label).
    """
    try:
        df = spark.sql(f"SELECT * FROM {hive_table}")
        count = df.count()
        if count > 0:
            print(f"[ML] Loaded {count:,} rows from Hive table `{hive_table}`.")
            return df, "hive"
        print(f"[ML] Hive table `{hive_table}` is empty; trying local CSV fallback...")
    except Exception as exc:
        print(f"[ML] Cannot read Hive table `{hive_table}` ({exc}); trying local CSV fallback...")

    root = _project_root()
    if fallback_dirs is None:
        # Prefer full simulated dataset (standard project data). Keep mock last (ETL smoke only).
        fallback_dirs = [
            os.path.join(root, "simulated-data-raw-csv"),
            os.path.join(root, "data", "simulated-data-raw-csv"),
            os.path.join(root, "data", "raw", "simulated-data-raw-csv"),
            os.path.join(root, "data", "mock"),
        ]

    root_path = _project_root()
    if root_path not in sys.path:
        sys.path.insert(0, root_path)
    # Import lazily to avoid circular import side effects at module load time.
    from src.etl_module_credit_transaction import clean_and_transform_data

    for directory in fallback_dirs:
        pattern = os.path.join(directory, "*.csv")
        import glob

        matches = glob.glob(pattern)
        if not matches:
            continue
        print(f"[ML] Loading & transforming CSV from '{directory}' ({len(matches)} files)...")
        df = clean_and_transform_data(spark, pattern)
        count = df.count()
        print(f"[ML] Loaded {count:,} rows from local CSV fallback.")
        return df, f"csv:{directory}"

    raise FileNotFoundError(
        "No ML dataset available. Run ETL into Hive from simulated-data-raw-csv, "
        "or place that folder at project root / data/simulated-data-raw-csv."
    )


def summarize_class_balance(df: DataFrame, label_col: str = LABEL_COL) -> DataFrame:
    """Return class counts and fraud rate for TX_FRAUD."""
    total = df.count()
    summary = (
        df.groupBy(label_col)
        .count()
        .withColumn("ratio", F.round(F.col("count") / F.lit(float(total)), 6))
        .orderBy(label_col)
    )
    return summary


def add_class_weights(
    df: DataFrame,
    label_col: str = LABEL_COL,
    weight_col: str = WEIGHT_COL,
) -> DataFrame:
    """
    Balanced class weighting: weight_c = n / (n_classes * n_c).

    Fraud (minority) class receives a higher weight so LogisticRegression
    does not ignore rare positive labels.
    """
    counts = {
        float(row[label_col]): int(row["count"])
        for row in df.groupBy(label_col).count().collect()
    }
    if len(counts) < 2:
        raise ValueError(
            f"Need both classes of `{label_col}` for training; found counts={counts}. "
            "Run ETL on the full simulated dataset (fraud labels required)."
        )

    n_total = sum(counts.values())
    n_classes = len(counts)
    weight_map = {
        label: float(n_total) / (n_classes * float(class_count))
        for label, class_count in counts.items()
    }

    print(f"[ML] Class counts: {counts}")
    print(f"[ML] Class weights: {weight_map}")

    weight_neg = weight_map.get(0.0, 1.0)
    weight_pos = weight_map.get(1.0, 1.0)
    return df.withColumn(
        weight_col,
        F.when(F.col(label_col) == F.lit(1.0), F.lit(weight_pos)).otherwise(F.lit(weight_neg)),
    )


def prepare_feature_frame(
    df: DataFrame,
    feature_cols: Optional[List[str]] = None,
    label_col: str = LABEL_COL,
) -> DataFrame:
    """
    Keep label + feature columns only; cast features to double; fill nulls with 0.
    """
    feature_cols = feature_cols or FEATURE_COLS
    missing = [c for c in feature_cols + [label_col] if c not in df.columns]
    if missing:
        raise ValueError(f"Dataset missing required ML columns: {missing}")

    selected = df.select(*feature_cols, label_col)
    for col_name in feature_cols:
        selected = selected.withColumn(col_name, F.coalesce(F.col(col_name).cast("double"), F.lit(0.0)))
    # Keep label as double 0.0/1.0 for MLlib classifiers & evaluators.
    selected = selected.withColumn(label_col, F.col(label_col).cast("double"))
    return selected


def build_ml_pipeline(
    feature_cols: Optional[List[str]] = None,
    label_col: str = LABEL_COL,
    weight_col: str = WEIGHT_COL,
    max_iter: int = 50,
    reg_param: float = 0.1,
    elastic_net_param: float = 0.0,
) -> Pipeline:
    """
    Build Spark ML Pipeline:
    VectorAssembler -> StandardScaler -> LogisticRegression(weightCol=...).
    """
    feature_cols = feature_cols or FEATURE_COLS

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol=ASSEMBLED_COL,
        handleInvalid="keep",
    )
    scaler = StandardScaler(
        inputCol=ASSEMBLED_COL,
        outputCol=FEATURES_COL,
        withStd=True,
        withMean=True,
    )
    logistic = LogisticRegression(
        featuresCol=FEATURES_COL,
        labelCol=label_col,
        weightCol=weight_col,
        maxIter=max_iter,
        regParam=reg_param,
        elasticNetParam=elastic_net_param,
        family="binomial",
    )
    return Pipeline(stages=[assembler, scaler, logistic])


def train_and_evaluate(
    df: DataFrame,
    train_ratio: float = 0.8,
    seed: int = 42,
    feature_cols: Optional[List[str]] = None,
) -> Tuple[PipelineModel, Dict[str, float], DataFrame, DataFrame]:
    """
    Split data, train Logistic Regression with class weights, evaluate PR-AUC & ROC-AUC.

    Returns: (model, metrics, train_df, test_predictions)
    """
    feature_cols = feature_cols or FEATURE_COLS
    prepared = prepare_feature_frame(df, feature_cols=feature_cols)
    weighted = add_class_weights(prepared)

    train_df, test_df = weighted.randomSplit([train_ratio, 1.0 - train_ratio], seed=seed)
    train_df = train_df.cache()
    test_df = test_df.cache()

    train_count = train_df.count()
    test_count = test_df.count()
    print(f"[ML] Train rows={train_count:,} | Test rows={test_count:,}")

    if train_df.select(LABEL_COL).distinct().count() < 2:
        raise ValueError("Training split contains only one class; use a larger fraud-labeled dataset.")
    if test_count == 0:
        raise ValueError("Test split is empty; increase dataset size.")

    pipeline = build_ml_pipeline(feature_cols=feature_cols)
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    metrics = evaluate_predictions(predictions)
    print(
        f"[ML] Evaluation on test set -> "
        f"Area Under PR = {metrics['areaUnderPR']:.6f} | "
        f"ROC-AUC = {metrics['areaUnderROC']:.6f}"
    )
    print("[ML] Note: Accuracy is intentionally not used (severe class imbalance).")
    return model, metrics, train_df, predictions


def evaluate_predictions(
    predictions: DataFrame,
    label_col: str = LABEL_COL,
) -> Dict[str, float]:
    """Compute Area Under PR-Curve and ROC-AUC via BinaryClassificationEvaluator."""
    pr_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )
    roc_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    return {
        "areaUnderPR": float(pr_evaluator.evaluate(predictions)),
        "areaUnderROC": float(roc_evaluator.evaluate(predictions)),
    }


def run_ml_pipeline(
    spark: Optional[SparkSession] = None,
    hive_table: str = HIVE_TABLE,
    fallback_dirs: Optional[List[str]] = None,
    stop_spark: bool = False,
) -> Tuple[PipelineModel, Dict[str, float]]:
    """
    End-to-end Phase 4 orchestration: load -> weight -> train -> evaluate.
    """
    owns_spark = spark is None
    if spark is None:
        spark = get_ml_spark_session()

    try:
        df, source = load_ml_dataset(spark, hive_table=hive_table, fallback_dirs=fallback_dirs)
        print(f"[ML] Dataset source: {source}")
        summarize_class_balance(df).show(truncate=False)
        model, metrics, _, _ = train_and_evaluate(df)
        return model, metrics
    finally:
        if owns_spark and stop_spark:
            spark.stop()


if __name__ == "__main__":
    # Allow: spark-submit src/ml_module.py [optional_csv_dir]
    extra_dir = sys.argv[1] if len(sys.argv) > 1 else None
    fallback = [extra_dir] if extra_dir else None
    run_ml_pipeline(fallback_dirs=fallback, stop_spark=True)
