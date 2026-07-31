# Dùng VS Code kết nối tới Jupyter Server trong Docker (Khuyên dùng nhất)

Bạn có thể viết notebook trực tiếp trên giao diện VS Code máy Local, nhưng cho code chạy thực thi bên trong môi trường Docker có đầy đủ PySpark & Java:

1. Mở file Notebook (.ipynb) trên VS Code ở máy Local.
2. Ở góc trên bên phải màn hình VS Code, bấm vào Select Kernel ➔ Chọn Existing Jupyter Server...
3. Dán đường dẫn URL sau vào
http://localhost:8888/lab?token=credit135
4. Chọn Python Kernel của container (Python 3 (ipykernel)). 
👉 Kết quả: Bạn được gõ code trên VS Code Local nhưng môi trường thực thi 100% nằm trong cụm Docker, sử dụng được ngay get_spark_session() và truy vấn Hive/HDFS cực kỳ mượt mà.