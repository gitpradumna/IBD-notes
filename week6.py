from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Week6_SparkSQL_SCD_Analysis")
    .getOrCreate()
)

bucket_name = "pradumna-iitm-w6"
transactions_path = f"gs://{bucket_name}/transactions.csv"
cust_master_path = f"gs://{bucket_name}/customer_master.csv"
cust_updates_path = f"gs://{bucket_name}/customer_updates.csv"
output_path = f"gs://{bucket_name}/output"

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW transactions
    USING csv
    OPTIONS (path '{transactions_path}', header 'true', inferSchema 'true')
""")

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW customer_master
    USING csv
    OPTIONS (path '{cust_master_path}', header 'true', inferSchema 'true')
""")

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW customer_updates
    USING csv
    OPTIONS (path '{cust_updates_path}', header 'true', inferSchema 'true')
""")

print("=== Input CSV files registered as temporary SparkSQL views ===")


# AGGREGATION QUERIES

# Query 1: Top 5 customers by total transaction amount
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW top_5_customers_result AS
    SELECT
        customer_id,
        ROUND(SUM(transaction_amount), 2) AS total_transaction_amount
    FROM transactions
    GROUP BY customer_id
    ORDER BY total_transaction_amount DESC
    LIMIT 5
""")

print("=== Query 1: Top 5 Customers by Total Transaction Amount ===")

spark.sql(f"""
    INSERT OVERWRITE DIRECTORY '{output_path}/top_5_customers'
    USING csv
    OPTIONS ('header' = 'true')
    SELECT *
    FROM top_5_customers_result
    ORDER BY total_transaction_amount DESC
""")


# Query 2: Number of customers per city
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW customers_per_city_result AS
    SELECT
        city,
        COUNT(DISTINCT customer_id) AS customer_count
    FROM transactions
    GROUP BY city
    ORDER BY customer_count DESC
""")

print("=== Query 2: Number of Customers per City ===")

spark.sql(f"""
    INSERT OVERWRITE DIRECTORY '{output_path}/customers_per_city'
    USING csv
    OPTIONS ('header' = 'true')
    SELECT *
    FROM customers_per_city_result
    ORDER BY customer_count DESC
""")


# Query 3: Average transaction value overall
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW avg_transaction_value_result AS
    SELECT
        ROUND(AVG(transaction_amount), 2) AS average_transaction_value
    FROM transactions
""")

print("=== Query 3: Average Transaction Value Overall ===")

spark.sql(f"""
    INSERT OVERWRITE DIRECTORY '{output_path}/avg_transaction_value'
    USING csv
    OPTIONS ('header' = 'true')
    SELECT *
    FROM avg_transaction_value_result
""")


# SLOWLY CHANGING DIMENSIONS - SCD TYPE I (OVERWRITE)

# Save the original customer master state
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW customer_master_scd1_before AS
    SELECT
        customer_id,
        customer_name,
        city,
        dob,
        CAST(effective_date AS DATE) AS effective_date,
        CAST(expiry_date AS DATE) AS expiry_date,
        CAST(current_flag AS INT) AS current_flag
    FROM customer_master
""")

print("=== SCD Type I: BEFORE State (Customer Master) ===")

spark.sql(f"""
    INSERT OVERWRITE DIRECTORY '{output_path}/scd_type1_customer_master/before'
    USING csv
    OPTIONS ('header' = 'true')
    SELECT *
    FROM customer_master_scd1_before
    ORDER BY customer_id
""")


# Apply SCD Type I updates by overwriting existing customer attributes
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW customer_master_scd1 AS
    SELECT
        COALESCE(u.customer_id, m.customer_id) AS customer_id,
        COALESCE(u.customer_name, m.customer_name) AS customer_name,
        COALESCE(u.city, m.city) AS city,
        COALESCE(CAST(u.dob AS DATE), CAST(m.dob AS DATE)) AS dob,
        CASE
            WHEN u.customer_id IS NOT NULL THEN CAST(u.change_date AS DATE)
            ELSE CAST(m.effective_date AS DATE)
        END AS effective_date,
        CAST(NULL AS DATE) AS expiry_date,
        1 AS current_flag
    FROM customer_master m
    FULL OUTER JOIN customer_updates u
        ON m.customer_id = u.customer_id
""")

print("=== SCD Type I: AFTER State (Customer Master Updated) ===")

spark.sql(f"""
    INSERT OVERWRITE DIRECTORY '{output_path}/scd_type1_customer_master/after'
    USING csv
    OPTIONS ('header' = 'true')
    SELECT *
    FROM customer_master_scd1
    ORDER BY customer_id
""")


# SLOWLY CHANGING DIMENSIONS - SCD TYPE II (HISTORY TRACKING)

# Create the original customer master snapshot
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW customer_master_scd2_before AS
    SELECT
        customer_id,
        customer_name,
        city,
        CAST(dob AS DATE) AS dob,
        CAST(effective_date AS DATE) AS effective_date,
        CAST(expiry_date AS DATE) AS expiry_date,
        CAST(current_flag AS INT) AS current_flag
    FROM customer_master
""")

print("=== SCD Type II: BEFORE State (Customer Master) ===")

spark.sql(f"""
    INSERT OVERWRITE DIRECTORY '{output_path}/scd_type2_customer_master/before'
    USING csv
    OPTIONS ('header' = 'true')
    SELECT *
    FROM customer_master_scd2_before
    ORDER BY customer_id, effective_date
""")


# Apply SCD Type II history tracking
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW customer_master_scd2_after AS

    -- 1. Unchanged active records and existing historical records
    SELECT
        m.customer_id,
        m.customer_name,
        m.city,
        m.dob,
        m.effective_date,
        m.expiry_date,
        m.current_flag
    FROM customer_master_scd2_before m
    LEFT JOIN customer_updates u
        ON m.customer_id = u.customer_id
    WHERE m.current_flag = 0
       OR u.customer_id IS NULL
       OR (
           m.customer_name <=> u.customer_name
           AND m.city <=> u.city
           AND m.dob <=> CAST(u.dob AS DATE)
       )

    UNION ALL

    -- 2. Expire previous active record for customers whose attributes changed
    SELECT
        m.customer_id,
        m.customer_name,
        m.city,
        m.dob,
        m.effective_date,
        DATE_SUB(CAST(u.change_date AS DATE), 1) AS expiry_date,
        0 AS current_flag
    FROM customer_master_scd2_before m
    INNER JOIN customer_updates u
        ON m.customer_id = u.customer_id
    WHERE m.current_flag = 1
      AND NOT (
          m.customer_name <=> u.customer_name
          AND m.city <=> u.city
          AND m.dob <=> CAST(u.dob AS DATE)
      )

    UNION ALL

    -- 3. Insert new active record for updated customers and brand-new customers
    SELECT
        u.customer_id,
        u.customer_name,
        u.city,
        CAST(u.dob AS DATE) AS dob,
        CAST(u.change_date AS DATE) AS effective_date,
        CAST(NULL AS DATE) AS expiry_date,
        1 AS current_flag
    FROM customer_updates u
    LEFT JOIN customer_master_scd2_before m
        ON u.customer_id = m.customer_id
       AND m.current_flag = 1
    WHERE m.customer_id IS NULL
       OR NOT (
           m.customer_name <=> u.customer_name
           AND m.city <=> u.city
           AND m.dob <=> CAST(u.dob AS DATE)
       )
""")

print("=== SCD Type II: AFTER State (Customer Master with History) ===")

spark.sql(f"""
    INSERT OVERWRITE DIRECTORY '{output_path}/scd_type2_customer_master/after'
    USING csv
    OPTIONS ('header' = 'true')
    SELECT *
    FROM customer_master_scd2_after
    ORDER BY customer_id, effective_date
""")


# FINAL VALIDATION QUERIES USING SPARKSQL

# Validation 1: SCD Type I row count
print("=== FINAL VALIDATION: SCD Type I row count ===")

spark.sql(f"""
    INSERT OVERWRITE DIRECTORY '{output_path}/validation/scd1_row_count'
    USING csv
    OPTIONS ('header' = 'true')
    SELECT
        COUNT(*) AS scd1_row_count
    FROM customer_master_scd1
""")


# Validation 2: SCD Type II total, current and historical row counts
print("=== FINAL VALIDATION: SCD Type II row counts ===")

spark.sql(f"""
    INSERT OVERWRITE DIRECTORY '{output_path}/validation/scd2_row_counts'
    USING csv
    OPTIONS ('header' = 'true')
    SELECT
        COUNT(*) AS total_scd2_rows,
        SUM(CASE WHEN current_flag = 1 THEN 1 ELSE 0 END) AS current_rows,
        SUM(CASE WHEN current_flag = 0 THEN 1 ELSE 0 END) AS historical_rows
    FROM customer_master_scd2_after
""")


# Validation 3: SCD Type II versions and current records per customer
print("=== FINAL VALIDATION: Type II records by customer ===")

spark.sql(f"""
    INSERT OVERWRITE DIRECTORY '{output_path}/validation/scd2_records_by_customer'
    USING csv
    OPTIONS ('header' = 'true')
    SELECT
        customer_id,
        COUNT(*) AS version_count,
        SUM(CASE WHEN current_flag = 1 THEN 1 ELSE 0 END)
            AS current_version_count
    FROM customer_master_scd2_after
    GROUP BY customer_id
    ORDER BY customer_id
""")


print("=== All SparkSQL queries, validations and GCS output writes completed ===")

spark.stop()