"""
Verification script to check credit transaction data imported into Hive and HDFS.
Outputs clean table info and suppresses Spark log output.
"""

import sys
import os
import logging

# Suppress py4j logging
logging.getLogger("py4j").setLevel(logging.ERROR)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.etl_module_credit_transaction import get_spark_session


def verify():
    spark = get_spark_session("VerifyCreditTransactionImport")
    
    # Suppress Spark JVM log output (INFO / WARN)
    spark.sparkContext.setLogLevel("ERROR")

    print("\n=== HIVE TABLES IN `credit_transaction_db` ===")
    spark.sql("SHOW TABLES IN credit_transaction_db").show(truncate=False)

    print("\n=== RECORD COUNT & FRAUD SUMMARY PER YEAR-MONTH ===")
    df_summary = spark.sql("""
        SELECT 
            tx_year_month, 
            count(*) as total_transactions, 
            count(DISTINCT CUSTOMER_ID) as unique_customers,
            count(DISTINCT TERMINAL_ID) as unique_terminals,
            sum(case when TX_FRAUD = 1 then 1 else 0 end) as total_fraud_tx,
            round(avg(TX_AMOUNT), 2) as avg_tx_amount,
            min(tx_date) as earliest_date, 
            max(tx_date) as latest_date 
        FROM credit_transaction_db.cleaned_transactions 
        GROUP BY tx_year_month 
        ORDER BY tx_year_month
    """)
    df_summary.show(15, truncate=False)

    print("\n=== SAMPLE CLEANED TRANSACTION DATA ===")
    spark.sql("""
        SELECT 
            TRANSACTION_ID, 
            TX_DATETIME, 
            CUSTOMER_ID, 
            TERMINAL_ID, 
            TX_AMOUNT, 
            TX_FRAUD, 
            tx_year_month,
            is_weekend,
            customer_tx_count 
        FROM credit_transaction_db.cleaned_transactions 
        LIMIT 5
    """).show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    verify()
