# Hướng Dẫn Thực Thức Phase 5: Advanced Analytics

> **Đồ án**: Credit Card Transaction Fraud Detection  
> **Người thực hiện (PIC)**: Nhật (Group 3)  
> **Notebook phân công**: [`notebooks/02_Advanced_Analysis.ipynb`](../notebooks/02_Advanced_Analysis.ipynb)  
> **Công nghệ sử dụng**: PySpark DataFrame API, Hive Warehouse, Matplotlib, Seaborn

---

## I. Tổng Quan Phase 5

Phase 5 tập trung vào việc **Phân tích Nâng cao theo Thời gian (Temporal Advanced Analytics)** và **Trực quan hóa Mẫu hình Gian lận**:
1. **Phân tích Tỷ lệ Gian lận theo Khung giờ (`tx_hour`)**: Xác định các khoảng thời gian ban đêm có nguy cơ gian lận cao đột biến.
2. **Phân tích Tỷ lệ Gian lận theo Ngày trong Tuần (`tx_day_of_week`)**: So sánh tỷ lệ gian lận giữa Ngày làm việc (Mon-Fri) và Ngày cuối tuần (Sat-Sun).
3. **Phân tích Biến động Tỷ lệ Gian lận theo Chuỗi Ngày (`tx_date`)**: Theo dõi các đợt bùng phát gian lận theo thời gian thực.
4. **Tổng hợp Nhận xét Chuyên sâu**: Cung cấp số liệu và hình ảnh trực quan phục vụ trực tiếp cho Slide thuyết trình và Báo cáo tổng kết đồ án (Phase 6).

---

## II. Hướng Dẫn Thực Thi

Bạn có thể chạy Phase 5 theo một trong hai cách dưới đây:

### 🚀 Cách 1: Chạy Chuẩn Trên Hạ Tầng Docker (Khuyên Dùng)

Đây là phương thức chạy đầy đủ luồng hệ thống Big Data: **HDFS ➔ Hive ➔ Spark Master/Worker ➔ JupyterLab**.

#### **Bước 1: Khởi động Cụm Docker Containers**
Mở PowerShell tại thư mục gốc dự án và chạy:
```powershell
docker compose -f docker/docker-compose.yml up -d
```
*Kiểm tra trạng thái các container bằng lệnh:*
```powershell
docker ps
```

#### **Bước 2: Chạy Spark DataFrame ETL để Nạp Dữ Liệu vào Hive**
Chạy script ETL PySpark để làm sạch và lưu dữ liệu vào bảng Hive `credit_transaction_db.cleaned_transactions`:
- **Chạy nhanh với dữ liệu Mock (2 tháng)**:
  ```powershell
  docker exec jupyter-lab spark-submit --driver-memory 4g /home/jovyan/src/etl_module_credit_transaction.py data/mock
  ```
- **Chạy full dữ liệu 24 tháng (`simulated-data-raw-csv`)**:
  ```powershell
  docker exec jupyter-lab spark-submit --driver-memory 4g /home/jovyan/src/etl_module_credit_transaction.py data/simulated-data-raw-csv
  ```

#### **Bước 3: Mở JupyterLab và Thực Thi Notebook**
1. Mở trình duyệt web và truy cập địa chỉ:  
   👉 **[http://localhost:8888/lab?token=credit135](http://localhost:8888/lab?token=credit135)**
2. Điều hướng tới thư mục **`notebooks/`** ➔ Mở file **`02_Advanced_Analysis.ipynb`**.
3. Chọn menu **Run ➔ Run All Cells** (hoặc tổ hợp phím `Shift + Enter` từng cell).

---

### 💻 Cách 2: Chạy Trực Tiếp Local Trên VS Code (Test Nhanh)

Nếu không bật cụm Docker, bạn vẫn có thể chạy thử nghiệm notebook ngay trong VS Code:

1. Mở file `notebooks/02_Advanced_Analysis.ipynb` trực tiếp trên VS Code.
2. Chọn Python Kernel thích hợp (đã cài đặt `pyspark`, `pandas`, `matplotlib`, `seaborn`).
3. Bấm **Run All**.

> 💡 **Cơ chế Tự Động Fallback**:  
> Notebook đã được lập trình sẵn cơ chế thông minh: Nếu không thể kết nối tới cụm Hive Metastore Docker, notebook sẽ tự động chuyển sang đọc file dữ liệu local CSV từ `data/simulated-data-raw-csv` hoặc `data/mock` để tạo Spark Session local và sinh đầy đủ các biểu đồ cho bạn.

---

## III. Cấu Trúc Các Cell Trong Notebook `02_Advanced_Analysis.ipynb`

| Cell ID | Loại Cell | Nội Dung & Chức Năng |
| :--- | :--- | :--- |
| **Cell 1** | Markdown | Giới thiệu tên Phase 5, mục tiêu bài toán và phân công người thực hiện (Nhật). |
| **Cell 2** | Code | Import các thư viện phụ thuộc (`pyspark.sql`, `matplotlib`, `seaborn`, `pandas`). |
| **Cell 3** | Code | Hàm `get_spark_session()` khởi tạo Spark Session với Hive Metastore support (có fallback local). |
| **Cell 4** | Code | Đọc dữ liệu từ Hive table `credit_transaction_db.cleaned_transactions` (hoặc fallback CSV local) bằng Spark DataFrame API. |
| **Cell 5 - 7** | Code & MD | Aggregation `groupBy("tx_hour")` + Biểu đồ **Line Chart** xu hướng tỷ lệ gian lận theo 24 giờ trong ngày. |
| **Cell 8 - 10**| Code & MD | Aggregation `groupBy("tx_day_of_week")` + Biểu đồ **Bar Chart** so sánh gian lận giữa Ngày làm việc (Weekday) và Ngày cuối tuần (Weekend). |
| **Cell 11 - 12**| Code & MD | Aggregation `groupBy("tx_date")` + Biểu đồ **Timeline Chart** biến động tỷ lệ gian lận theo chuỗi thời gian. |
| **Cell 13** | Markdown | Tóm tắt kết quả phân tích chuyên sâu (Executive Summary & Insights) dùng cho Slide & Report. |

---

## IV. Kết Quả Trực Quan Hóa Đầu Ra

Sau khi thực thi xong notebook, bạn sẽ thu được các biểu đồ trực quan chính:
1. **Line Chart (24h Trend)**: Hiển thị tỷ lệ gian lận tăng vọt vào khung giờ ban đêm (0h - 5h sáng), với chú thích mũi tên highlight đỉnh điểm gian lận.
2. **Bar Chart (Weekday vs Weekend)**: Phân biệt tỷ lệ gian lận giữa ngày làm việc (màu xanh cyan) và ngày cuối tuần (màu vàng da cam).
3. **Daily Timeline Chart**: Biểu diễn diễn biến gian lận hàng ngày so với đường mốc trung bình.

---

## V. Xử Lý Lỗi Thường Gặp (Troubleshooting)

1. **Lỗi `AnalysisException: Table or view not found`**:
   - *Nguyên nhân*: Chưa chạy script ETL nạp dữ liệu ở Bước 2.
   - *Khắc phục*: Chạy lại câu lệnh `docker exec jupyter-lab spark-submit ...` hoặc để notebook tự động fallback đọc dữ liệu local.
2. **Lỗi `OutOfMemoryError` khi chạy trên Spark**:
   - *Khắc phục*: Thêm cờ `--driver-memory 4g` khi spark-submit hoặc giảm `spark.sql.shuffle.partitions` xuống `8` trong notebook.
