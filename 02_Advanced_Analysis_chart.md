### 1. Tổng quan Chỉ số KPI (Fraud Risk Executive Dashboard)
> **Mục tiêu**: Giúp người đọc/báo cáo nắm bắt ngay thông số toàn cảnh về rủi ro tài chính ngay khi mở notebook.

* **Chỉ số thống kê bổ sung**:
  * **Tổng số giao dịch** & **Tổng số giao dịch gian lận** (`count`).
  * **Tỷ lệ gian lận tổng thể (%)** (`Overall Fraud Rate`).
  * **Tổng thiệt hại tài chính ($)** (`SUM(TX_AMOUNT)` của gian lận).
  * **Giá trị giao dịch gian lận trung bình ($)** (`AVG(TX_AMOUNT)` gian lận) vs **Giao dịch hợp lệ ($)**.
  * **Tỷ lệ tổn thất doanh thu (% Financial Loss)** = `Tổng $ gian lận / Tổng $ giao dịch`.
* **Trực quan hóa**: 
  * Bổ sung **KPI Summary Cards** (dùng Matplotlib `fig.add_axes()` hoặc HTML/Markdown formatting) ở ngay đầu Section 5.

---

### 2. Phân tích Chi tiết Kịch bản Gian lận (`TX_FRAUD_SCENARIO`)
> **Mục tiêu**: Phân loại bản chất của gian lận (ví dụ: gian lận giá trị cao, tấn công trạm gian lận, hay hành vi bất thường).

* **Nội dung phân tích & Thống kê**:
  * Thống kê số lượng vụ và tổng thiệt hại tài chính phân theo từng `TX_FRAUD_SCENARIO` (Scenario 0, 1, 2, 3...).
  * Tỷ trọng % của từng loại kịch bản trong tổng số các vụ gian lận.
* **Loại biểu đồ đề xuất**:
  * **Donut Chart / Pie Chart**: Thể hiện cơ cấu tỷ trọng các kịch bản gian lận.
  * **Horizontal Grouped Bar Chart**: So sánh tổng giá trị tổn thất ($) vs Số lượng vụ theo từng kịch bản.

---

### 3. Phân tích Phân phối Giá trị Giao dịch & Thiệt hại Tài chính (`TX_AMOUNT`)
> **Mục tiêu**: Tìm hiểu khoảng giá trị giao dịch nào thường bị kẻ gian nhắm tới nhất.

* **Thống kê bổ sung**:
  * Tính toán các chỉ số mô tả: **Mean, Median, StdDev, Min, P25, P75, P95, Max** của `TX_AMOUNT` tách biệt cho 2 nhóm: `Fraud (1)` vs `Non-Fraud (0)`.
  * Xác định ngưỡng giao dịch rủi ro cao (e.g., giao dịch trên $200 có tỷ lệ gian lận tăng bao nhiêu %).
* **Loại biểu đồ đề xuất**:
  * **Boxplot / Violin Plot (Log Scale)**: So sánh độ phân tán và ngoại lệ (outliers) của giá trị giao dịch thường vs gian lận.
  * **KDE / Histogram Plot**: Biểu đồ phân phối mật độ giá trị giao dịch.

---

### 4. Ma trận Rủi ro Giờ x Ngày trong Tuần (Heatmap 2D: `tx_hour` x `tx_day_of_week`)
> **Mục tiêu**: Phát hiện "Điểm nóng gian lận" (Fraud Hotspots) kết hợp giữa giờ và thứ.

* **Nội dung phân tích**:
  * Gom nhóm 2 chiều `groupBy('tx_day_of_week', 'tx_hour')` để tính tỷ lệ gian lận %.
* **Loại biểu đồ đề xuất**:
  * **Seaborn Heatmap (24h x 7 ngày)**: Dùng bảng màu tương phản (Color Palette như `YlOrRd` hoặc `Magma`) giúp nhận diện lập tức khung giờ nguy hiểm nhất (ví dụ: Thứ 7 / Chủ Nhật từ 1h - 4h sáng).

---

### 5. Phân tích Thực thể Rủi ro Cao: Trạm Giao dịch (`TERMINAL_ID`) & Khách hàng (`CUSTOMER_ID`)
> **Mục tiêu**: Nhận diện các điểm bán hàng (POS/Terminal) hoặc tài khoản khách hàng bị thỏa hiệp (compromised).

* **Nội dung phân tích & Thống kê**:
  * **Terminal Risk**: Top 10/20 Terminal có số vụ gian lận cao nhất hoặc tỷ lệ gian lận 100%.
  * **Customer Behavioral Anomaly**: So sánh số tiền giao dịch hiện tại (`TX_AMOUNT`) với mức chi tiêu trung bình lịch sử của khách hàng (`customer_avg_amount`).
* **Loại biểu đồ đề xuất**:
  * **Top 10 High-Risk Terminals (Horizontal Bar Chart)**: Hiển thị 10 Terminal nguy hiểm nhất.
  * **Scatter Plot (Độ lệch giao dịch)**: Trực quan độ lệch `TX_AMOUNT - customer_avg_amount` đối với các giao dịch bị đánh dấu gian lận.

---

### 6. Phân tích Kết hợp Đêm & Cuối tuần (`is_night` x `is_weekend`)
> **Mục tiêu**: Đánh giá tác động của yếu tố phi hành chính đối với gian lận.

* **Nội dung phân tích & Thống kê**:
  * Phân tích tỷ lệ gian lận theo 4 nhóm ma trận:
    1. Ngày thường - Ban ngày (`is_weekend=0, is_night=0`)
    2. Ngày thường - Ban đêm (`is_weekend=0, is_night=1`)
    3. Cuối tuần - Ban ngày (`is_weekend=1, is_night=0`)
    4. Cuối tuần - Ban đêm (`is_weekend=1, is_night=1`)
* **Loại biểu đồ đề xuất**:
  * **Grouped Bar Chart (Clustered Bar Chart)** hoặc **Stacked Bar Chart** thể hiện rõ sự nhảy vọt rủi ro vào ban đêm cuối tuần.

---

### 7. Tóm tắt Cấu trúc Notebook Cải tiến Đề xuất

| STT | Mục / Section | Phân tích & Chỉ số bổ sung | Loại biểu đồ |
| :--- | :--- | :--- | :--- |
| **1** | **Executive Summary** | KPI Summary (Total Tx, Fraud Tx, Total Loss $, Avg Fraud Amount, Loss Rate %) | KPI Cards Block |
| **2** | **Ma trận Rủi ro Giờ x Thứ (New)** | Tỷ lệ gian lận kết hợp 24h x 7 ngày | **Seaborn Heatmap** |
| **3** | **Phân tích Phân phối Giá trị Giao dịch (`TX_AMOUNT`) (New)** | Median, Mean, Outliers, Total $ Financial Loss | **Boxplot / Violin Plot & Dual-Axis Chart** |
| **4** | **Phân tích Kịch bản Gian lận (`TX_FRAUD_SCENARIO`) (New)** | Tỷ trọng % & tổn thất $ theo từng Scenario | **Donut Chart & Horizontal Bar Chart** |
| **5** | **Phân tích Thực thể Rủi ro (`TERMINAL_ID`) (New)** | Top 10 Terminals bị tấn công nhiều nhất | **Horizontal Bar Chart** |
| **6** | **Phân tích Kết hợp Đêm & Cuối tuần (New)** | Tỷ lệ gian lận kết hợp đêm & cuối tuần | **Horizontal Bar Chart** |

---

