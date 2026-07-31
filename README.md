# Credit Card Fraud Detection

## I. Problem Statement
- Subject: Credit Card Fraud Detection</br>
https://www.kaggle.com/mlg-ulb/creditcardfraud
Description: The dataset contains information about transactions made by credit cardholders, including fraudulent and non-fraudulent transactions.
Suggested implementation:
- Use Spark SQL to analyze transactions by time or customer.
- Identify transactions with the fastest spread.
- Use Hive to store and query analytics tables by customer.
- Use MLlib for Machine Learning (clustering).
- Use GraphFrames for graph analysis.


## II. Setup Environment

### 2.1. Create containers
- Create virtual environment
```bash
# Create virtual environment
python -m venv .venv
```
- Activate virtual environment
```bash
# Activate virtual environment
.\.venv\Scripts\activate
```
- Install dependencies
```bash
# Install dependencies
pip install -r requirements.txt
```
- Start Docker containers
```bash
# Start Docker containers
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


## III. Data Ingestion & Processing (ETL Pipeline)

### 3.1. Run ETL Script
- Run with Mock Dataset (recommended for fast testing):
```bash
docker exec jupyter-lab spark-submit --driver-memory 4g /home/jovyan/src/etl_module_credit_transaction.py data/mock
```
- Put data files of credit transaction fraud inside folder data/raw

- Run with Full Raw Dataset:
```bash
docker exec jupyter-lab spark-submit --driver-memory 4g /home/jovyan/src/etl_module_credit_transaction.py data/raw/credit_transaction_fraud
```

### 3.2. Verify Imported Data

- Run Verification Script:
```bash
docker exec jupyter-lab spark-submit --driver-java-options "-Dlog4j.logLevel=ERROR" /home/jovyan/tests/verify_credit_transaction_import.py
```

- Run Schema Unit Tests:
```bash
pytest tests/test_credit_transaction_etl.py -v
```

- Check HDFS Parquet Partition Directories:
```bash
docker exec namenode hdfs dfs -ls /credit_transaction/processed/parquet
```

