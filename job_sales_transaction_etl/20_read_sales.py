# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M001: Load POS Transactions
# MAGIC %md
# MAGIC # 02_read_sales: Load POS Transactions from Bronze (M002)
# MAGIC
# MAGIC This notebook reads validated POS transactions from the Bronze layer:
# MAGIC * Reads from `bronze.pos_transactions` Delta table (raw data landing zone)
# MAGIC * Filters by current batch/job_date
# MAGIC * Applies strict schema casting from STRING to proper types
# MAGIC * **E003 Error:** Throws ValueError on schema casting failures
# MAGIC * Validates data types and formats
# MAGIC
# MAGIC **Dependencies:** Run `00_initialize` first to set up parameters
# MAGIC
# MAGIC **Schema:**
# MAGIC * transaction_id: STRING
# MAGIC * customer_id: STRING
# MAGIC * product_id: STRING
# MAGIC * store_id: STRING
# MAGIC * quantity: INT
# MAGIC * unit_price: DECIMAL(10,2)
# MAGIC * discount_amount: DECIMAL(10,2)
# MAGIC * transaction_time: TIMESTAMP
# MAGIC
# MAGIC **Output:** `df_sales` - Strictly validated POS transactions DataFrame

# COMMAND ----------

# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Read Sales Transactions
# M002: Read POS Sales from Bronze Layer with Schema Casting
start_time = datetime.now()

bronze_table = f"{catalog_name}.bronze.pos_transactions"
print(f"Reading POS transactions from Bronze: {bronze_table}")
print(f"Filtering by: _job_date = {job_date}, _batch_id = {batch_id}")

try:
    # Read from Bronze layer (filtered by job_date and batch_id)
    df_bronze_raw = spark.read.table(bronze_table) \
        .filter(F.col("_job_date") == job_date) \
        .filter(F.col("_batch_id") == batch_id)
    
    # Check if data exists for this batch
    if df_bronze_raw.count() == 0:
        error_message = f"E002: No data found in Bronze for job_date={job_date}, batch_id={batch_id}"
        print(f"\n✗ {error_message}")
        raise ValueError(error_message)
    
    print(f"✓ Found {df_bronze_raw.count():,} records in Bronze layer")
    
    # Apply strict schema casting from STRING to proper types
    print("\nApplying schema casting...")
    
    df_sales_raw = df_bronze_raw.select(
        F.col("transaction_id").cast("string").alias("transaction_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("store_id").cast("string").alias("store_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        F.col("discount_amount").cast("decimal(10,2)").alias("discount_amount"),
        F.col("transaction_time").cast("timestamp").alias("transaction_time")
    )
    
    record_count = df_sales_raw.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Successfully cast {record_count:,} records to proper schema")
    log_metrics("read_from_bronze", record_count, duration)
    
except Exception as e:
    # E003: Schema casting error
    if "cast" in str(e).lower() or "type" in str(e).lower():
        error_message = f"E003: Schema casting failed reading from Bronze\nDetails: {str(e)}"
        print(f"\n✗ {error_message}")
        raise ValueError(error_message)
    else:
        print(f"✗ Error reading from Bronze: {str(e)}")
        raise

# Store as df_sales for backward compatibility
df_sales = df_sales_raw

# COMMAND ----------

# DBTITLE 1,Display Sample Data
# Display POS transaction schema and sample
print("\n" + "="*60)
print("POS TRANSACTIONS SCHEMA")
print("="*60)
df_sales.printSchema()

print("\n" + "="*60)
print("SAMPLE POS TRANSACTIONS")
print("="*60)
display(df_sales.limit(10))

print(f"\nTotal Records: {df_sales.count():,}")
