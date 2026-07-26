# Trending YouTube Video Statistics

## I. Problem Statement
- Subject: Trending YouTube Video Statistics</br>
https://www.kaggle.com/datasets/datasnaek/youtube-new
Description: The dataset contains information about trending videos on YouTube by country (title, views, likes, category, etc.).
Suggested implementation:
- Use Spark SQL to analyze trends over time or by country.
- Identify videos with the fastest spread.
- Use Hive to store and query analytics tables by country.
- Use MLib for Machine Learning (clustering).
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
  | **JupyterLab** | [http://localhost:8888/lab?token=youtube135](http://localhost:8888/lab?token=youtube135) | Token: `youtube135` | IDE to write and execute PySpark / Data analysis scripts |
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
docker exec jupyter-lab spark-submit /home/jovyan/src/etl_module.py data/mock
```

- Run with Full Raw Dataset:
```bash
docker exec jupyter-lab spark-submit /home/jovyan/src/etl_module.py data/raw/trending_youtube
```

### 3.2. Verify Imported Data

- Run Verification Script:
```bash
docker exec jupyter-lab spark-submit --driver-java-options "-Dlog4j.logLevel=ERROR" /home/jovyan/tests/verify_import.py
```

- Run Schema Unit Tests:
```bash
pytest tests/test_etl_schema.py -v
```

- Check HDFS Parquet Partition Directories:
```bash
docker exec namenode hdfs dfs -ls /youtube/processed/parquet
```

- **Query Cleaned Data in JupyterLab / PySpark**:
```python
import sys, os
sys.path.append(os.path.abspath(".."))

from src.etl_module import get_spark_session

spark = get_spark_session()
spark.sql("SELECT country, count(*) as total_videos FROM youtube_db.cleaned_videos GROUP BY country").show()
```
