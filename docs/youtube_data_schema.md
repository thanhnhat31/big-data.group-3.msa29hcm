# Data Schema Contract: Hive Data Warehouse (`youtube_db.cleaned_videos`)

## 1. Overview
Documenting the schema contract for cleaned and transformed YouTube Trending data after PySpark ETL execution.

### Data Flow Architecture
1. **Raw Layer**: `hdfs://namenode:9000/youtube/raw/csv/*.csv` & `hdfs://namenode:9000/youtube/raw/json/*.json`
2. **Intermediate Processed Layer**: `hdfs://namenode:9000/youtube/processed/parquet/` (Parquet Snappy format)
3. **Hive Warehouse Layer**: Hive External/Managed Table `youtube_db.cleaned_videos` (ORC / Parquet format)

## 2. Table Schema & Column Specifications

| Column Name | Data Type | Partition Key | Description | Example |
| :--- | :--- | :--- | :--- | :--- |
| `video_id` | `STRING` | No | ID duy nhất của video | `2kyS6SvSYSE` |
| `trending_date` | `DATE` | No | Ngày video lên tab trending | `2017-11-14` |
| `title` | `STRING` | No | Tiêu đề video | `WE WANT TO TALK ABOUT OUR MARRIAGE` |
| `channel_title` | `STRING` | No | Tên kênh đăng tải | `CaseyNeistat` |
| `category_id` | `INT` | No | ID danh mục video | `22` |
| `category_title` | `STRING` | No | Tên danh mục được map từ JSON | `People & Blogs` |
| `publish_time` | `TIMESTAMP` | No | Thời điểm đăng tải video | `2017-11-13 17:13:01` |
| `publish_date` | `DATE` | No | Ngày đăng video | `2017-11-13` |
| `publish_hour` | `INT` | No | Giờ đăng video (0 - 23) | `17` |
| `publish_day_of_week`| `STRING` | No | Thứ đăng video (Mon, Tue...) | `Mon` |
| `tags` | `STRING` | No | Chuỗi tag gốc (phân cách bởi \|) | `"tag1"\|"tag2"` |
| `tags_array` | `ARRAY<STRING>` | No | Mảng danh sách các tag | `["tag1", "tag2"]` |
| `views` | `BIGINT` | No | Lượt xem | `748374` |
| `likes` | `BIGINT` | No | Lượt thích | `57527` |
| `dislikes` | `BIGINT` | No | Lượt không thích | `2966` |
| `comment_count` | `BIGINT` | No | Số lượng bình luận | `15954` |
| `thumbnail_link` | `STRING` | No | URL ảnh thumbnail | `https://i.ytimg.com/vi/...` |
| `comments_disabled` | `BOOLEAN` | No | Tắt tính năng bình luận | `false` |
| `ratings_disabled` | `BOOLEAN` | No | Tắt tính năng đánh giá | `false` |
| `video_error_or_removed`| `BOOLEAN` | No | Video bị xóa hoặc lỗi | `false` |
| `description` | `STRING` | No | Mô tả chi tiết video | `"SHANTELL'S CHANNEL..."` |
| `days_to_trend` | `INT` | No | Số ngày từ lúc publish -> trending | `1` |
| `engagement_rate` | `DOUBLE` | No | Tỷ lệ tương tác: `(likes+dislikes+comments)/views` | `0.102` |
| `like_ratio` | `DOUBLE` | No | Tỷ lệ thích: `likes / (likes + dislikes)` | `0.951` |
| `total_days_trending`| `INT` | No | Tổng số ngày video giữ thứ hạng trending | `6` |
| `country` | `STRING` | **YES** | Mã quốc gia (US, GB, CA, DE...) | `US` |
| `trending_year_month`| `STRING` | **YES** | Phân vùng năm-tháng trending | `2017-11` |

## 3. Hive DDL Statement
```sql
CREATE DATABASE IF NOT EXISTS youtube_db;

CREATE EXTERNAL TABLE IF NOT EXISTS youtube_db.cleaned_videos (
    video_id STRING,
    trending_date DATE,
    title STRING,
    channel_title STRING,
    category_id INT,
    category_title STRING,
    publish_time TIMESTAMP,
    publish_date DATE,
    publish_hour INT,
    publish_day_of_week STRING,
    tags STRING,
    tags_array ARRAY<STRING>,
    views BIGINT,
    likes BIGINT,
    dislikes BIGINT,
    comment_count BIGINT,
    thumbnail_link STRING,
    comments_disabled BOOLEAN,
    ratings_disabled BOOLEAN,
    video_error_or_removed BOOLEAN,
    description STRING,
    days_to_trend INT,
    engagement_rate DOUBLE,
    like_ratio DOUBLE,
    total_days_trending INT
)
PARTITIONED BY (
    country STRING,
    trending_year_month STRING
)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/youtube_db.db/cleaned_videos'
TBLPROPERTIES ("orc.compress"="SNAPPY");
```
