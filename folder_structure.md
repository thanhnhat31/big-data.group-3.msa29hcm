credit-transaction-fraud-bigdata/
├── data/                               # Dữ liệu cục bộ (Được quản lý trong .gitignore)
│   ├── raw/                            # Dữ liệu thô gốc
│   └── mock/                           # Dữ liệu mẫu thu nhỏ
├── docker/                             # Cấu hình hạ tầng containerized
│   ├── docker-compose.yml              # Khởi tạo cụm HDFS, Spark Master/Worker, Hive Metastore & Server, JupyterLab
│   └── hadoop_conf/                    # Các file cấu hình (core-site.xml, hdfs-site.xml, hive-site.xml)
├── docs/                               # Tài liệu thiết kế, quy chuẩn dữ liệu & sơ đồ hệ thống
│   ├── data_schema/                    # Schema và định nghĩa cấu trúc bảng dữ liệu
│   │   └── credit_data_schema.md       # Định nghĩa chi tiết schema bảng Hive credit_transaction_db.cleaned_transactions
│   └── diagram/                        # Sơ đồ thiết kế kiến trúc và pipeline (PlantUML)
│   │   ├── eda_pipeline.puml           # Sơ đồ quy trình EDA
│   │   ├── etl_pipeline.puml           # Sơ đồ quy trình ETL
│   │   └── ml_pipeline.puml            # Sơ đồ quy trình huấn luyện Machine Learning
├── notebooks/                          # Jupyter Notebooks trình diễn & thực nghiệm trực quan
│   ├── images/                         # Sơ đồ, biểu đồ và hình ảnh minh họa cho notebooks
│   ├── 00_SimulatedDataset.ipynb       # Khám phá cấu trúc dữ liệu mô phỏng
│   ├── 01_Behavioral_Analysis.ipynb    # Phân tích hành vi giao dịch & mẫu hình gian lận
│   ├── 02_Advanced_Analysis.ipynb      # Phân tích nâng cao hành vi giao dịch (thời gian, địa điểm, RFM)
│   ├── 03_Machine_Learning.ipynb       # Huấn luyện & đánh giá mô hình ML dự đoán gian lận
│   ├── 04_Graph_Analysis.ipynb         # Phân tích đồ thị mạng lưới liên kết giao dịch bằng GraphFrames
│   └── 05_Spark_GraphFrame_Demo.ipynb  # Minh họa thực thi GraphFrames trên Spark
├── src/                                # Thư mục chứa module xử lý lõi PySpark (Spark DataFrame API)
│   ├── etl_module_credit_transaction.py # Ingestion HDFS, làm sạch, biến đổi Spark DataFrame & lưu Hive Warehouse
│   └── ml_module.py                    # Pipeline Spark MLlib (VectorAssembler & mô hình dự đoán gian lận)
├── tests/                              # Bộ kiểm thử unit test & xác minh dữ liệu ETL/ML
│   ├── test_credit_transaction_etl.py  # Unit test PySpark DataFrame schema, null handling & calculations
│   ├── test_etl_schema.py              # Kiểm thử kiểm tra tính đúng đắn của schema ETL
│   ├── test_ml_pipeline.py             # Unit test cho Pipeline Machine Learning
│   └── verify_credit_transaction_import.py # Script kiểm tra số lượng bản ghi & thống kê trong Hive
├── .gitignore                          # Loại trừ các file dữ liệu thô dung lượng lớn & checkpoints
├── pytest.ini                          # Cấu hình khung kiểm thử pytest
├── README.md                           # Hướng dẫn khởi chạy hệ thống & thực thi Spark DataFrame ETL
├── requirements.txt                    # Thư viện phụ thuộc: pyspark, pandas, plotly, pytest...