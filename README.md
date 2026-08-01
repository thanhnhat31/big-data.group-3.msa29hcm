# Credit Card Transaction Fraud Detection

## I. Problem Statement
- **Subject**: Credit Card Transaction Fraud Detection
- **Description**: The dataset contains simulated credit card transaction data generated on a monthly basis (from April 2018 to March 2020). It includes transaction metadata (`TRANSACTION_ID`, `TX_DATETIME`, `CUSTOMER_ID`, `TERMINAL_ID`, `TX_AMOUNT`), datetime indicators, and ground truth fraud labels (`TX_FRAUD`, `TX_FRAUD_SCENARIO`).
- **Implementation Approach**:
  - **Spark DataFrame API**: Perform high-performance distributed data ingestion, data cleaning, schema casting, null handling, and windowed feature aggregations (`customer_tx_count`, `customer_avg_amount`, `terminal_tx_count`, `is_weekend`, `is_night`).
  - **Hive Data Warehouse**: Store processed transaction data in Parquet format partitioned by transaction year-month (`tx_year_month`).
  - **Spark MLlib**: Train machine learning models for fraud classification and risk scoring with class weighting and evaluator metrics (`areaUnderPR`, `areaUnderROC`).
  - **GraphFrames**: Analyze transaction network graphs between customers (`CUSTOMER_ID`) and terminals (`TERMINAL_ID`).
- **Presentation Notebooks**:
  - `01_Behavioral_Analysis.ipynb`: Analyze behaviors of customers and terminals to detect fraudulent activities.
  - `02_Advanced_Analysis.ipynb`: Analyze fraud rate patterns across hours, days of the week, timeline, and spatial features.
  - `03_Machine_Learning.ipynb`: Apply Spark MLlib to train models for fraud detection and classification.
  - `04_Graph_Analysis.ipynb`: Apply GraphFrames to analyze transaction network graphs for fraud detection.

## II. Setup Environment

### 2.1. Create containers
- Create virtual environment:
```bash
python -m venv .venv
```
- Activate virtual environment (Windows PowerShell):
```bash
.\.venv\Scripts\activate
```
- Install dependencies:
```bash
pip install -r requirements.txt
```
- Start Docker containers:
```bash
docker compose -f docker/docker-compose.yml up -d
```

### 2.2. Verify Environment
- Verify Docker containers are running:
```bash
docker ps
```

- **Web User Interfaces (Web UI)**:
  | Service | Web UI URL | Default Credentials / Token | Purpose |
  | :--- | :--- | :--- | :--- |
  | **HDFS NameNode** | [http://localhost:9870](http://localhost:9870) | N/A | Check HDFS storage status & browse filesystem |
  | **Spark Master** | [http://localhost:8080](http://localhost:8080) | N/A | Monitor Spark cluster, workers & running applications |
  | **JupyterLab** | [http://localhost:8888/lab?token=credit135](http://localhost:8888/lab?token=credit135) | Token: `credit135` | IDE to write and execute PySpark / Data analysis scripts |
  | **Spark App UI** | [http://localhost:4040](http://localhost:4040) | N/A | Detailed DAG & Task progress for active Spark jobs |

- **Service Connection Ports**:
  - **HiveServer2 (JDBC SQL)**: `localhost:10000`
  - **Hive Metastore (Thrift RPC)**: `localhost:9083`
  - **HDFS Master (RPC)**: `localhost:9000`
  - **Spark Master (RPC)**: `spark://spark-master:7077` (Port `7077`)


## III. Data Ingestion, Processing & Modeling Pipelines

### 3.1. Run ETL Script (Spark DataFrame)
- Run with Mock Dataset (recommended for fast local testing):
```bash
docker exec jupyter-lab spark-submit --driver-memory 4g /home/jovyan/src/etl_module_credit_transaction.py data/mock
```

- Run with Full Raw Simulated Monthly Dataset:
```bash
docker exec jupyter-lab spark-submit --driver-memory 4g /home/jovyan/src/etl_module_credit_transaction.py data/raw/credit_transaction_fraud
```

### 3.2. Run Machine Learning Pipeline (Spark MLlib)
- Run Spark MLlib model training & evaluation script:
```bash
docker exec jupyter-lab spark-submit --driver-memory 4g /home/jovyan/src/ml_module.py
```

### 3.3. Verify Imported Data & Run Test Suite
- Run Hive Import & Record Count Verification Script:
```bash
docker exec jupyter-lab spark-submit --driver-java-options "-Dlog4j.logLevel=ERROR" /home/jovyan/tests/verify_credit_transaction_import.py
```

- Run All PySpark & ML Unit Tests:
```bash
pytest -v
```

- Check HDFS Parquet Partition Directories:
```bash
docker exec namenode hdfs dfs -ls /credit_transaction/processed/parquet
```

## IV. Data Analysis & Presentation Notebooks

The directory `notebooks/` contains Jupyter Notebooks for data simulation, behavioral EDA, ML modeling, and graph analytics:

1. `00_SimulatedDataset.ipynb`:
   - Simulates credit card transaction datasets with realistic parameters and fraud scenarios.

2. `01_Behavioral_Analysis.ipynb`:
   - Analyzes customer and terminal behavior to identify transaction anomalies and fraud patterns.

3. `02_Advanced_Analysis.ipynb`:
   - Analyzes fraud rate distributions across hours, days of the week, timeline, and feature risk scores.

4. `03_Machine_Learning.ipynb`:
   - Trains and evaluates Spark MLlib classifiers (Logistic Regression, Random Forest, GBT) using PR-AUC and ROC-AUC metrics.

5. `04_Graph_Analysis.ipynb`:
   - Builds Customer-Terminal transaction bipartite graphs using GraphFrames to analyze network connectivity and fraud clusters.

6. `05_Spark_GraphFrame_Demo.ipynb`:
   - Demonstrates GraphFrames setup, Motif finding, and PageRank / Connected Components algorithms for presentation.

## V. Project Architecture & Documentation References

- **Folder Structure**: Refer to [folder_structure.md](folder_structure.md) for the complete directory overview.
- **Hive Table Schema**: Refer to [docs/data_schema/credit_data_schema.md](docs/data_schema/credit_data_schema.md) for full schema definitions, field descriptions, and partitioning rules.
- **System Architecture & Pipeline Diagrams**:
  - [EDA Pipeline Diagram](docs/diagram/eda_pipeline.puml)
  - [ETL Pipeline Diagram](docs/diagram/etl_pipeline.puml)
  - [ML Pipeline Diagram](docs/diagram/ml_pipeline.puml)
