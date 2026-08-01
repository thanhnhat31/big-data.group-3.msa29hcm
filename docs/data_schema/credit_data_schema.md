# Data Schema Contract: Hive Data Warehouse (`credit_transaction_db.cleaned_transactions`)

## 1. Overview
Documenting the schema contract for cleaned and transformed Credit Card Transaction Fraud data after PySpark ETL execution.

### Data Flow Architecture
1. **Raw Layer**: `hdfs://namenode:9000/credit_transaction/raw/csv/*.csv`
2. **Intermediate Processed Layer**: `hdfs://namenode:9000/credit_transaction/processed/parquet/` (Parquet Snappy format)
3. **Hive Warehouse Layer**: Hive External/Managed Table `credit_transaction_db.cleaned_transactions` (Parquet / ORC format)

---

## 2. Table Schema & Column Specifications

| Column Name | Data Type | Partition Key | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `TRANSACTION_ID` | `BIGINT` | No | ID duy nhất của giao dịch credit card | `0` |
| `TX_DATETIME` | `TIMESTAMP` | No | Thời điểm thực hiện giao dịch (YYYY-MM-DD HH:MM:SS) | `2018-04-01 00:00:17` |
| `CUSTOMER_ID` | `BIGINT` | No | ID duy nhất của khách hàng | `6160` |
| `TERMINAL_ID` | `BIGINT` | No | ID duy nhất của thiết bị/trạm POS | `24605` |
| `TX_AMOUNT` | `DOUBLE` | No | Số tiền giao dịch | `31.83` |
| `TX_TIME_SECONDS` | `BIGINT` | No | Thời gian giao dịch tính theo giây từ mốc bắt đầu | `17` |
| `TX_TIME_DAYS` | `INT` | No | Thời gian giao dịch tính theo ngày | `0` |
| `TX_FRAUD` | `INT` | No | Nhãn gian lận (0: Bình thường, 1: Gian lận) | `0` |
| `TX_FRAUD_SCENARIO` | `INT` | No | Kịch bản/Loại hình gian lận (0, 1, 2, 3...) | `0` |
| `tx_date` | `DATE` | No | Ngày thực hiện giao dịch (YYYY-MM-DD) | `2018-04-01` |
| `tx_hour` | `INT` | No | Giờ thực hiện giao dịch (0 - 23) | `0` |
| `tx_day_of_week` | `STRING` | No | Thứ trong tuần (Mon, Tue, Wed, Thu, Fri, Sat, Sun) | `Sun` |
| `is_weekend` | `INT` | No | Cờ đánh dấu ngày cuối tuần (1: Có, 0: Không) | `1` |
| `is_night` | `INT` | No | Cờ đánh dấu giao dịch ban đêm (0h - 6h) (1: Có, 0: Không) | `1` |
| `customer_tx_count` | `BIGINT` | No | Số lượng giao dịch của khách hàng trong tháng | `25` |
| `customer_avg_amount` | `DOUBLE` | No | Số tiền giao dịch trung bình của khách hàng trong tháng | `45.50` |
| `terminal_tx_count` | `BIGINT` | No | Số lượng giao dịch qua terminal trong tháng | `12` |
| `tx_year_month` | `STRING` | **YES** | Phân vùng năm-tháng thực hiện giao dịch (YYYY-MM) | `2018-04` |

---

## 3. Hive DDL Statement

```sql
CREATE DATABASE IF NOT EXISTS credit_transaction_db;

CREATE EXTERNAL TABLE IF NOT EXISTS credit_transaction_db.cleaned_transactions (
    TRANSACTION_ID BIGINT,
    TX_DATETIME TIMESTAMP,
    CUSTOMER_ID BIGINT,
    TERMINAL_ID BIGINT,
    TX_AMOUNT DOUBLE,
    TX_TIME_SECONDS BIGINT,
    TX_TIME_DAYS INT,
    TX_FRAUD INT,
    TX_FRAUD_SCENARIO INT,
    tx_date DATE,
    tx_hour INT,
    tx_day_of_week STRING,
    is_weekend INT,
    is_night INT,
    customer_tx_count BIGINT,
    customer_avg_amount DOUBLE,
    terminal_tx_count BIGINT
)
PARTITIONED BY (
    tx_year_month STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/credit_transaction/processed/parquet'
TBLPROPERTIES ("parquet.compress"="SNAPPY");
```
