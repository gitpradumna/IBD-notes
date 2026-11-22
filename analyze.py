from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, month, initcap, trim, to_date, sum, avg, desc

# 1. Initialize SparkSession (bypassing Lakehouse REST catalog)
spark = SparkSession.builder \
    .appName("CustomerTransactionAnalysis") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.execution.datasources.v2.SessionCatalog") \
    .getOrCreate()

# Update bucket name if needed
bucket_name = "pradumna-iitm-w5"
cust_path = f"gs://{bucket_name}/customer_dataset.csv"
trans_path = f"gs://{bucket_name}/transaction_dataset.csv"

# Load Datasets
cust_df = spark.read.csv(cust_path, header=True, inferSchema=True)
trans_df = spark.read.csv(trans_path, header=True, inferSchema=True)

# ---------------------------------------------------------
# 2. CLEAN CUSTOMERS
# ---------------------------------------------------------
cust_df = cust_df.withColumn("city", trim(col("city")))
cust_df = cust_df.withColumn(
    "city",
    when(col("city").ilike("cmbt"), "Coimbatore")
    .when(col("city").ilike("cochin"), "Kochi")
    .otherwise(initcap(col("city")))
)

cust_invalid_cond = (
    col("customer_id").isNull() | (trim(col("customer_id")) == "") |
    col("city").isNull() | (trim(col("city")) == "") |
    col("status").isNull() | (trim(col("status")) == "")
)

invalid_customers = cust_df.filter(cust_invalid_cond)
valid_customers = cust_df.filter(~cust_invalid_cond).dropDuplicates(["customer_id"])

# Cache valid customers to break execution DAG loop
valid_customers.cache()
valid_customers.count()

# ---------------------------------------------------------
# 3. CLEAN TRANSACTIONS
# ---------------------------------------------------------
trans_df = trans_df.withColumn(
    "transaction_month", month(col("transaction_date").cast("date"))
).withColumn(
    "amount_category",
    when(col("transaction_amount") < 3000, "Low")
    .when((col("transaction_amount") >= 3000) & (col("transaction_amount") <= 7000), "Medium")
    .otherwise("High")
)

trans_invalid_cond = (
    col("transaction_id").isNull() | (trim(col("transaction_id")) == "") |
    col("customer_id").isNull() | (trim(col("customer_id")) == "") |
    col("transaction_amount").isNull() | (col("transaction_amount") <= 0) |
    to_date(col("transaction_date"), "yyyy-MM-dd").isNull()
)

invalid_transactions_initial = trans_df.filter(trans_invalid_cond)
valid_transactions_initial = trans_df.filter(~trans_invalid_cond).dropDuplicates(["transaction_id"])

# Referential Integrity: check customer exists
valid_cust_ids = valid_customers.select("customer_id")
valid_transactions = valid_transactions_initial.join(valid_cust_ids, on="customer_id", how="inner")
orphaned_transactions = valid_transactions_initial.join(valid_cust_ids, on="customer_id", how="left_anti")

invalid_transactions = invalid_transactions_initial.unionByName(orphaned_transactions)

# Cache dataframes in memory before write actions
valid_transactions.cache()
valid_transactions.count()
invalid_transactions.cache()
invalid_transactions.count()

# ---------------------------------------------------------
# 4. JOINS & AGGREGATIONS
# ---------------------------------------------------------
joined_data = valid_transactions.join(valid_customers, on="customer_id", how="inner").cache()

customer_totals = joined_data.groupBy("customer_id", "customer_name").agg(
    sum("transaction_amount").alias("total_transaction_amount"),
    avg("transaction_amount").alias("avg_transaction_amount")
)

city_totals = joined_data.groupBy("city").agg(
    sum("transaction_amount").alias("total_city_amount")
)

top_3_customers = customer_totals.orderBy(desc("total_transaction_amount")).limit(3)

# ---------------------------------------------------------
# 5. LOGGING & OUTPUT WRITES
# ---------------------------------------------------------
print("--- Sample of Invalid Records (Transactions) ---")
invalid_transactions.show(5)

print("--- Sample of Cleaned & Joined Data ---")
joined_data.show(5)

# Write outputs to GCS
valid_customers.coalesce(1).write.csv(f"gs://{bucket_name}/cleaned_data/customers", header=True, mode="overwrite")
valid_transactions.coalesce(1).write.csv(f"gs://{bucket_name}/cleaned_data/transactions", header=True, mode="overwrite")

invalid_customers.coalesce(1).write.csv(f"gs://{bucket_name}/invalid_rows/customers", header=True, mode="overwrite")
invalid_transactions.coalesce(1).write.csv(f"gs://{bucket_name}/invalid_rows/transactions", header=True, mode="overwrite")

joined_data.coalesce(1).write.csv(f"gs://{bucket_name}/joined_data", header=True, mode="overwrite")

customer_totals.coalesce(1).write.csv(f"gs://{bucket_name}/aggregates/customer_totals", header=True, mode="overwrite")
city_totals.coalesce(1).write.csv(f"gs://{bucket_name}/aggregates/city_totals", header=True, mode="overwrite")
top_3_customers.coalesce(1).write.csv(f"gs://{bucket_name}/aggregates/top_3_customers", header=True, mode="overwrite")

spark.stop()