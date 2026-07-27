"""
Verification script to check data imported into Hive and HDFS.
Outputs only clean table info and suppresses Spark log output.
"""

import sys
import os
import logging

# Suppress py4j logging
logging.getLogger("py4j").setLevel(logging.ERROR)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.etl_module import get_spark_session

def verify():
    spark = get_spark_session("VerifyImport")
    
    # Suppress Spark JVM log output (INFO / WARN)
    spark.sparkContext.setLogLevel("ERROR")

    print("\n=== HIVE TABLES IN `youtube_db` ===")
    spark.sql("SHOW TABLES IN youtube_db").show(truncate=False)

    print("\n=== RECORD COUNT & DATE RANGES PER COUNTRY ===")
    df_summary = spark.sql("""
        SELECT 
            country, 
            count(*) as total_records, 
            count(DISTINCT video_id) as unique_videos,
            min(trending_date) as earliest_trending, 
            max(trending_date) as latest_trending 
        FROM youtube_db.cleaned_videos 
        GROUP BY country 
        ORDER BY country
    """)
    df_summary.show(15, truncate=False)

    print("\n=== SAMPLE CLEANED DATA ===")
    spark.sql("""
        SELECT 
            video_id, 
            country, 
            title, 
            category_title, 
            publish_time, 
            days_to_trend,
            engagement_rate 
        FROM youtube_db.cleaned_videos 
        LIMIT 5
    """).show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    verify()
