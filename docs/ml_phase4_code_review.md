# Báo Cáo Code Review: Phase 4 - Machine Learning (Spark MLlib)

> **Đồ án**: Credit Card Transaction Fraud Detection  
> **Người thực hiện**: Tuấn (Group 3)  
> **Người review**: Nhật (Group 3)  
> **Gửi tới**: Anh Thanh & Nhóm 3  
> **Tệp tin được review**:
> - [`notebooks/03_Machine_Learning.ipynb`](../notebooks/03_Machine_Learning.ipynb)
> - [`src/ml_module.py`](../src/ml_module.py)
> - [`tests/test_ml_pipeline.py`](../tests/test_ml_pipeline.py)

---

## I. Tóm Tắt Đánh Giá Chung (Executive Summary)

| Hạng mục đánh giá | Trạng thái | Ghi chú |
| :--- | :---: | :--- |
| **Tính đúng đắn so với đề bài (Requirements)** | ✅ PASS | Đạt 100% yêu cầu từ 4.1 đến 4.5 trong bảng phân công. |
| **Chuẩn Kiến trúc Big Data (Architecture)** | ✅ PASS | Tách bạch module lõi (`src/ml_module.py`), gọi trong Notebook và có Unit Test đầy đủ. |
| **Phòng chống Rò rỉ Nhãn (Label Leakage)** | ✅ PASS | Đã loại bỏ `TX_FRAUD_SCENARIO` và các cột định danh (`TRANSACTION_ID`, `CUSTOMER_ID`, `TERMINAL_ID`). |
| **Xử lý Mất cân bằng Dữ liệu (Class Imbalance)** | ✅ PASS | Áp dụng công thức Class Weighting chuẩn cho `LogisticRegression`. |
| **Đánh giá Mô hình (Evaluation Metrics)** | ✅ PASS | Ưu tiên **Area Under PR-Curve** và **ROC-AUC** thay vì Accuracy. |

---

## II. Chi Tiết Kết Quả Review Theo Từng Mục Phân Công

### 1. [Phase 4.1] Truy xuất Dữ liệu (Data Ingestion)
- **Kiểm tra**:
  - Code ưu tiên đọc trực tiếp từ bảng Hive `credit_transaction_db.cleaned_transactions`.
  - Có cơ chế Fallback đọc từ thư mục dữ liệu chuẩn `simulated-data-raw-csv` (24 tháng, chứa 14,158,971 dòng và 35,564 giao dịch gian lận).
  - Không sử dụng `data/mock` để huấn luyện mô hình (vì `data/mock` chỉ dùng để test ETL smoke).
- **Đánh giá**: **ĐẠT (PASS)**. Logic nạp dữ liệu rõ ràng, có fallback an toàn.

---

### 2. [Phase 4.2] Feature Engineering & Pipeline (`VectorAssembler` + `StandardScaler`)
- **Kiểm tra**:
  - Danh sách đặc trưng được chọn đúng 8 biến theo thiết kế:
    `['TX_AMOUNT', 'TX_TIME_SECONDS', 'tx_hour', 'is_weekend', 'is_night', 'customer_tx_count', 'customer_avg_amount', 'terminal_tx_count']`.
  - Sử dụng Spark `Pipeline` với 3 stages đóng gói hoàn chỉnh:
    1. `VectorAssembler` ➔ Gom các cột đặc trưng thành cột `assembled_features`.
    2. `StandardScaler` ➔ Chuẩn hóa dữ liệu về cùng quy mô (Standardization với `withMean=True, withStd=True`).
    3. `LogisticRegression` ➔ Mô hình phân loại nhị phân.
- **Đánh giá**: **ĐẠT (PASS)**. Đóng gói trong PySpark `Pipeline` giúp chống Data Leakage giữa tập Train và Test.

---

### 3. [Phase 4.3] Xử lý Mất cân bằng Dữ liệu (Class Weighting)
- **Kiểm tra**:
  - Do tỷ lệ gian lận trong dữ liệu thực tế chỉ chiếm **~0.25%** (35,564 / 14,158,971), mô hình dễ bị lệch về nhãn đa số.
  - Sử dụng công thức Class Weighting chuẩn:
    $$w_c = \frac{N_{total}}{N_{classes} \times N_c}$$
  - Kết quả gán trọng số: Nhãn gian lận (`TX_FRAUD = 1`) nhận trọng số cao hơn hẳn (~199.06) so với nhãn bình thường (`TX_FRAUD = 0`: ~0.501).
  - Trọng số được truyền trực tiếp vào tham số `weightCol=class_weight` của `LogisticRegression`.
- **Đánh giá**: **ĐẠT (PASS)**. Giải pháp xử lý mất cân bằng hiệu quả và đúng chuẩn MLlib.

---

### 4. [Phase 4.4 & 4.5] Huấn luyện Mô hình & Đánh giá (Evaluation)
- **Kiểm tra**:
  - Chia tập dữ liệu Train/Test theo tỷ lệ 80/20 với cố định `seed=42`.
  - Có thực hiện `.cache()` cho `train_df` và `test_df` để tối ưu tốc độ tính toán của Spark.
  - Sử dụng `BinaryClassificationEvaluator` với 2 chỉ số chính:
    - **Area Under PR-Curve (`areaUnderPR`)**: đạt **`0.468655`** (Chỉ số quan trọng nhất cho dữ liệu mất cân bằng).
    - **ROC-AUC (`areaUnderROC`)**: đạt **`0.806468`**.
  - Không sử dụng Accuracy làm metric chính (vì Accuracy bị "ảo" ở mức 99.7% do dữ liệu quá lệch).
- **Đánh giá**: **ĐẠT (PASS)**. Phương pháp đánh giá chính xác, khách quan.

---

## III. Điểm Mạnh & Đóng Góp Nổi Bật Của Code

1. **Tính Tái Sử Dụng Cao (Modular Code)**: Tất cả hàm xử lý ML đều được định nghĩa sạch sẽ trong [`src/ml_module.py`](../src/ml_module.py), cho phép gọi cả từ CLI (`spark-submit`), từ PyTest lẫn trong Jupyter Notebook.
2. **Bộ Unit Test Đầy Đủ**: File [`tests/test_ml_pipeline.py`](../tests/test_ml_pipeline.py) kiểm thử tự động toàn bộ contract đặc trưng, tính toán trọng số lớp, pipeline stages và evaluators.
3. **Trình Bày Trực Quan Trọng Notebook**: File [`notebooks/03_Machine_Learning.ipynb`](../notebooks/03_Machine_Learning.ipynb) có cấu trúc mạch lạc, có output thực thi mẫu và chú thích markdown rõ ràng cho từng bước.

---

## IV. Đề Xuất Nâng Cao (Optional Recommendations Cho Phase 6)

1. **Phân tích Confusion Matrix**:
   - Ở mục 4.5, notebook đã in ra bảng thống kê dự đoán (True Positives, False Positives, True Negatives, False Negatives). Có thể bổ sung 1 biểu đồ Heatmap Confusion Matrix để đưa vào Slide thuyết trình.
2. **So sánh mô hình bổ sung (Nếu làm phần Mở rộng cho Report)**:
   - Ngoài `LogisticRegression`, có thể thử nghiệm thêm `RandomForestClassifier` hoặc `GBTClassifier` (Gradient-Boosted Trees) trong Spark MLlib để so sánh chỉ số PR-AUC.

---

## V. Kết Luận
Bản code Phase 4 do **Tuấn** thực hiện đạt chất lượng tốt, hoàn toàn tuân thủ thiết kế chung của nhóm và sẵn sàng nghiệm thu để đưa số liệu vào Slide & Báo cáo đồ án (Phase 6).

**Người lập báo cáo review**: Nhật (Group 3)
