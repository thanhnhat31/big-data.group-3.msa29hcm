youtube-trending-bigdata/
├── data/                       # Dữ liệu cục bộ (Sẽ được thiết lập block trong .gitignore)
│   ├── raw/                    # Dữ liệu gốc tải từ tập youtube-new (CSV, JSON)
│   └── mock/                   # Dữ liệu mẫu thu nhỏ (VD: 10MB) để test script nhanh trên local
├── docker/                     # Phục vụ Phase 1 
│   ├── docker-compose.yml      # File cấu hình khởi tạo cụm HDFS, Spark, Hive trên nền Linux
│   └── hadoop_conf/            # Các file config (core-site.xml, hdfs-site.xml) cho cluster
├── src/                        # Chứa các hàm xử lý lõi (Tránh viết code dài dòng trong Notebook)
│   ├── etl_module.py           # Phase 2: Script đẩy HDFS, làm sạch, ép kiểu và lưu Hive
│   ├── ml_module.py            # Phase 4: Pipeline VectorAssembler và K-Means
│   └── graph_module.py         # Phase 5: Script tạo Vertices, Edges và chạy thuật toán đồ thị
├── notebooks/                  # Phục vụ Phase 6 (Presentation Layers - Trình diễn trực quan)
│   ├── 01_Basic_Analysis.ipynb     # Mapping với Phase 6.1 (Thống kê mô tả)
│   ├── 02_Advanced_Analysis.ipynb  # Mapping với Phase 6.2 (Thời gian, không gian, hành vi)
│   ├── 03_Machine_Learning.ipynb   # Mapping với Phase 6.3 (Kết quả gom cụm video)
│   ├── 04_Graph_Processing.ipynb   # Mapping với Phase 6.4 (Mạng lưới đồ thị)
│   └── Master_Presentation.ipynb   # Kịch bản chính import các hàm từ src/ 
├── tests/                      # Chốt chặn kiểm soát chất lượng dữ liệu 
│   ├── test_etl_schema.py      # Script tự động kiểm tra data có bị Null hay sai kiểu không
│   └── test_ml_pipeline.py     # Script kiểm tra model K-Means có output ra đúng Cluster_ID
├── docs/                       # Phục vụ Phase 7
│   ├── Data_Schema_Contract.md # Tài liệu cực kỳ quan trọng: Định nghĩa các cột của bảng Hive
│   └── System_Architecture.png # Sơ đồ kiến trúc triển khai
├── .gitignore                  # Block thư mục data/raw, .ipynb_checkpoints, .DS_Store
├── requirements.txt            # pyspark, pandas, plotly, pyvis, pytest...
└── README.md                   # Hướng dẫn clone code và khởi động môi trường