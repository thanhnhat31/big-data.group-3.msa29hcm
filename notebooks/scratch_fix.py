import json

file_path = "e:/GitHub/big-data.group-3.msa29hcm/notebooks/04_Spark_GraphFrame_Demo.ipynb"

with open(file_path, "r", encoding="utf-8") as f:
    nb = json.load(f)

for cell in nb["cells"]:
    if cell["cell_type"] == "code":
        source = cell["source"]
        new_source = []
        for line in source:
            if "from pyspark.sql.functions import col, lit" in line:
                line = line.replace("import col, lit", "import col, lit, concat")
            elif "giả sử bảng tên là 'credit_transactionscleaned_transactionscredit_transaction_db.cleaned_transactions'" in line:
                line = line.replace("credit_transactionscleaned_transactionscredit_transaction_db.cleaned_transactions", "credit_transaction_db.cleaned_transactions")
            elif 'df = spark.read.parquet("hdfs://localhost:9000/user/hive/warehouse/fraud_transactions_silver/")' in line:
                line = 'df = spark.table("credit_transaction_db.cleaned_transactions")\n'
                
                # We can also add a fallback as comment
                new_source.append("# Đọc trực tiếp qua Spark SQL (vì đã có enableHiveSupport)\n")
            elif 'customers = df.select(col("CUSTOMER_ID").cast("string").alias("id"))' in line:
                line = line.replace('col("CUSTOMER_ID").cast("string")', 'concat(lit("C_"), col("CUSTOMER_ID").cast("string"))')
            elif 'terminals = df.select(col("TERMINAL_ID").cast("string").alias("id"))' in line:
                line = line.replace('col("TERMINAL_ID").cast("string")', 'concat(lit("T_"), col("TERMINAL_ID").cast("string"))')
            elif 'edges = df.withColumnRenamed("CUSTOMER_ID", "src") \\' in line:
                line = 'edges = df.withColumn("src", concat(lit("C_"), col("CUSTOMER_ID").cast("string"))) \\\n'
            elif '.withColumnRenamed("TERMINAL_ID", "dst")' in line:
                line = '          .withColumn("dst", concat(lit("T_"), col("TERMINAL_ID").cast("string")))\n'
            elif 'hdfs://localhost:9000/user/spark/checkpoints' in line:
                line = line.replace('localhost', 'namenode')
            
            new_source.append(line)
        cell["source"] = new_source

with open(file_path, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)
    f.write("\n")
