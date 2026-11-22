from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, month, initcap, trim, to_date, sum, avg, desc

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("CustomerTransactionAnalysis").getOrCreate()

# Update this with your actual GCS bucket name
bucket_name = "YOUR_BUCKET_NAME"
cust_path = f"gs://{bucket_name}/customer_dataset.csv"
trans_path = f"gs://{bucket_name}/transaction_dataset.csv"

# Load Datasets
cust_df = spark.read.csv(cust_path, header=True, inferSchema=True)
trans_df = spark.read.csv(trans_path, header=True, inferSchema=True)

# ---------------------------------------------------------
# 2. COLUMN TRANSFORMATIONS & 3. DATA CLEANING
# ---------------------------------------------------------

# --- Clean Customer Data ---
# Standardize city names: trim spaces, fix custom typos (cmbt -> Coimbatore), apply Title Case
cust_df = cust_df.withColumn("city", trim(col("city")))
cust_df = cust_df.withColumn(
    "city",
    when(col("city").ilike("cmbt"), "Coimbatore")
    .when(col("city").ilike("cochin"), "Kochi")
    .otherwise(initcap(col("city")))
)

# Identify invalid customers (missing key fields)
cust_invalid_cond = (
    col("customer_id").isNull() | (trim(col("customer_id")) == "") |
    col("city").isNull() | (trim(col("city")) == "") |
    col("status").isNull() | (trim(col("status")) == "")
)

# Split into valid and invalid, removing exact duplicates from valid
invalid_customers = cust_df.filter(cust_invalid_cond)
valid_customers = cust_df.filter(~cust_invalid_cond).dropDuplicates()


# --- Clean Transaction Data ---
# Extract month and create amount categorization
trans_df = trans_df.withColumn(
    "transaction_month", month(col("transaction_date").cast("date"))
).withColumn(
    "amount_category",
    when(col("transaction_amount") < 3000, "Low")
    .when((col("transaction_amount") >= 3000) & (col("transaction_amount") <= 7000), "Medium")
    .otherwise("High")
)

# Identify invalid transactions (missing fields, negative amount, invalid date)
trans_invalid_cond = (
    col("transaction_id").isNull() | (trim(col("transaction_id")) == "") |
    col("customer_id").isNull() | (trim(col("customer_id")) == "") |
    col("transaction_amount").isNull() | (col("transaction_amount") <= 0) |
    to_date(col("transaction_date"), "yyyy-MM-dd").isNull()
)

invalid_transactions_initial = trans_df.filter(trans_invalid_cond)
valid_transactions_initial = trans_df.filter(~trans_invalid_cond).dropDuplicates()

# Ensure Referential Integrity: Keep only transactions where customer_id exists in valid_customers
valid_customer_ids = valid_customers.select("customer_id").distinct()

# Final valid transactions
valid_transactions = valid_transactions_initial.join(valid_customer_ids, on="customer_id", how="inner")

# Isolate orphaned transactions (valid format, but customer_id doesn't exist in customer dataset)
orphaned_transactions = valid_transactions_initial.join(valid_customer_ids, on="customer_id", how="left_anti")

# Combine all invalid transactions
invalid_transactions = invalid_transactions_initial.unionByName(orphaned_transactions)


# ---------------------------------------------------------
# 4. JOINS
# ---------------------------------------------------------
joined_data = valid_transactions.join(valid_customers, on="customer_id", how="inner")


# ---------------------------------------------------------
# 5. AGGREGATIONS
# ---------------------------------------------------------
# Total and average transaction amount per customer
customer_totals = joined_data.groupBy("customer_id", "customer_name").agg(
    sum("transaction_amount").alias("total_transaction_amount"),
    avg("transaction_amount").alias("avg_transaction_amount")
)

# Total transaction amount per city
city_totals = joined_data.groupBy("city").agg(
    sum("transaction_amount").alias("total_city_amount")
)

# Top 3 customers by total transaction amount
top_3_customers = customer_totals.orderBy(desc("total_transaction_amount")).limit(3)


# ---------------------------------------------------------
# 6. LOGGING & OUTPUTS
# ---------------------------------------------------------
print("--- Sample of Invalid Records (Transactions) ---")
invalid_transactions.show(5)

print("--- Sample of Cleaned & Joined Data ---")
joined_data.show(5)

# Save outputs to GCS. Using coalesce(1) forces a single CSV file per directory for easier assignment download.
valid_customers.coalesce(1).write.csv(f"gs://{bucket_name}/cleaned_data/customers", header=True, mode="overwrite")
valid_transactions.coalesce(1).write.csv(f"gs://{bucket_name}/cleaned_data/transactions", header=True, mode="overwrite")

invalid_customers.coalesce(1).write.csv(f"gs://{bucket_name}/invalid_rows/customers", header=True, mode="overwrite")
invalid_transactions.coalesce(1).write.csv(f"gs://{bucket_name}/invalid_rows/transactions", header=True, mode="overwrite")

joined_data.coalesce(1).write.csv(f"gs://{bucket_name}/joined_data", header=True, mode="overwrite")

customer_totals.coalesce(1).write.csv(f"gs://{bucket_name}/aggregates/customer_totals", header=True, mode="overwrite")
city_totals.coalesce(1).write.csv(f"gs://{bucket_name}/aggregates/city_totals", header=True, mode="overwrite")
top_3_customers.coalesce(1).write.csv(f"gs://{bucket_name}/aggregates/top_3_customers", header=True, mode="overwrite")

spark.stop()
