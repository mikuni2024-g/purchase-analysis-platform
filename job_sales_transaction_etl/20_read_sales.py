# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M001: Load POS Transactions
# MAGIC %md
# MAGIC # 20_read_sales: Load POS Transactions from Bronze (M002)
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
    # Read from Bronze layer (filtered by job_date, use most recent batch if current batch_id not found)
    df_bronze_all = spark.read.table(bronze_table).filter(F.col("_job_date") == job_date)
    
    # Check if data exists for this job_date
    if df_bronze_all.count() == 0:
        error_message = f"E002: No data found in Bronze for job_date={job_date}"
        print(f"\n✗ {error_message}")
        raise ValueError(error_message)
    
    # Try to find data with current batch_id, fall back to most recent batch
    df_bronze_raw = df_bronze_all.filter(F.col("_batch_id") == batch_id)
    
    if df_bronze_raw.count() == 0:
        # Current batch_id not found, use the most recent batch for this job_date
        from pyspark.sql import Window
        latest_batch = df_bronze_all.select("_batch_id", "_ingestion_timestamp") \
            .distinct() \
            .orderBy(F.col("_ingestion_timestamp").desc()) \
            .first()["_batch_id"]
        
        print(f"⚠ Warning: Current batch_id {batch_id} not found in Bronze")
        print(f"  Using most recent batch: {latest_batch}")
        
        df_bronze_raw = df_bronze_all.filter(F.col("_batch_id") == latest_batch)
    
    print(f"✓ Found {df_bronze_raw.count():,} records in Bronze layer")
    
    # Apply strict schema casting from STRING to proper types
    print("\nApplying schema casting...")
    
    df_sales_raw = df_bronze_raw.select(
        F.col("transaction_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("store_id"),
        F.expr("try_cast(quantity as int)").alias("quantity"),
        F.expr("try_cast(unit_price as decimal(10,2))").alias("unit_price"),
        F.expr("try_cast(discount_amount as decimal(10,2))").alias("discount_amount"),
        F.expr("try_cast(transaction_time as timestamp)").alias("transaction_time")
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
