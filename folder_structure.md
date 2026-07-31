credit-transaction-fraud-bigdata/
├── data/                               # Dữ liệu cục bộ (Được quản lý trong .gitignore)
│   ├── raw/                            # Dữ liệu thô gốc hoặc simulated-data-raw-csv
│   │   └── simulated-data-raw-csv/     # Dữ liệu mô phỏng giao dịch 24 tháng (transactions_month_2018-04.csv -> 2020-03.csv)
│   └── mock/                           # Dữ liệu mẫu thu nhỏ (2 tháng) để test script nhanh trên local
├── docker/                             # Cấu hình hạ tầng containerized
│   ├── docker-compose.yml              # Khởi tạo cụm HDFS, Spark Master/Worker, Hive Metastore & Server, JupyterLab
│   └── hadoop_conf/                    # Các file cấu hình (core-site.xml, hdfs-site.xml, hive-site.xml)
├── src/                                # Thư mục chứa module xử lý lõi PySpark (Spark DataFrame API)
│   ├── etl_module_credit_transaction.py # Ingestion HDFS, làm sạch, biến đổi Spark DataFrame & lưu Hive Warehouse
│   ├── ml_module.py                    # Pipeline Spark MLlib (VectorAssembler & mô hình dự đoán gian lận)
│   └── graph_module.py                 # Spark GraphFrames (Tạo Vertices, Edges & phân tích mạng lưới rủi ro)
├── notebooks/                          # Jupyter Notebooks trình diễn trực quan
│   ├── 00_SimulatedDataset.ipynb       # Khám phá cấu trúc dữ liệu mô phỏng
│   ├── 01_Behavioral_Analysis.ipynb    # Phân tích hành vi giao dịch & mẫu hình gian lận
│   ├── 03_Machine_Learning.ipynb       # Huấn luyện & đánh giá mô hình ML dự đoán gian lận
│   └── 04_Spark_GraphFrame_Demo.ipynb  # Phân tích mạng lưới liên kết Customer - Terminal
├── tests/                              # Bộ kiểm thử chất lượng dữ liệu & xác minh ETL
│   ├── test_credit_transaction_etl.py  # Unit test PySpark DataFrame schema, null handling & calculations
│   └── verify_credit_transaction_import.py # Script kiểm tra số lượng bản ghi & thống kê trong Hive
├── docs/                               # Tài liệu thiết kế & quy chuẩn dữ liệu
│   └── credit_data_schema.md           # Định nghĩa chi tiết schema bảng Hive credit_transaction_db.cleaned_transactions
├── .gitignore                          # Loại trừ các file dữ liệu thô dung lượng lớn & checkpoints
├── requirements.txt                    # Thư viện phụ thuộc: pyspark, pandas, plotly, pytest...
└── README.md                           # Hướng dẫn khởi chạy hệ thống & thực thi Spark DataFrame ETL