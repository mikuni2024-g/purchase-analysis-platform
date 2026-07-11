# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M002: Load Master Tables
# MAGIC %md
# MAGIC # 02_read_master: Load Master Tables (M002)
# MAGIC
# MAGIC This notebook handles:
# MAGIC * Loading reference/master data tables
# MAGIC * **Customer master** (JSON) - cached for repeated access
# MAGIC * **Product master** (CSV) - broadcast-optimized for joins
# MAGIC * **Store master** (CSV) - store dimension data
# MAGIC * **Retail calendar** (CSV) - time dimension data
# MAGIC
# MAGIC **Dependencies:** Run `00_initialize` first
# MAGIC
# MAGIC **Output:** Master DataFrames:
# MAGIC * `df_customer_master` - Customer dimension (cached)
# MAGIC * `df_product_master` - Product dimension (broadcast hint)
# MAGIC * `df_store_master` - Store dimension
# MAGIC * `df_retail_calendar` - Retail calendar dimension

# COMMAND ----------

# DBTITLE 1,Run Initialization
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Read Product Master
# M002: Load product master (CSV) - Optimized for Broadcast joins
start_time = datetime.now()

product_path = f"{input_path}/erp/product_master.csv"
print(f"Reading product master from: {product_path}")

try:
    df_product_master = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .format("csv") \
        .load(product_path)
    
    # Optimize for broadcast joins (product master is typically small)
    # Mark for broadcast hint - will be used in downstream joins
    df_product_master = df_product_master.hint("broadcast")
    
    product_count = df_product_master.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Loaded {product_count:,} products (broadcast-optimized)")
    log_metrics("read_product_master", product_count, duration)
    
except Exception as e:
    print(f"✗ Error reading product master: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Read Customer Master
# M002: Load customer master (JSON) - Apply cache optimization
start_time = datetime.now()

customer_path = f"{input_path}/crm/customer_master.json"
print(f"Reading customer master from: {customer_path}")

try:
    df_customer_master = spark.read \
        .option("multiLine", "true") \
        .format("json") \
        .load(customer_path)
    
    customer_count = df_customer_master.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Loaded {customer_count:,} customers")
    log_metrics("read_customer_master", customer_count, duration)
    
except Exception as e:
    print(f"✗ Error reading customer master: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Read Store Master
# M002: Load store master (CSV)
start_time = datetime.now()

store_path = f"{input_path}/erp/store_master.csv"
print(f"Reading store master from: {store_path}")

try:
    df_store_master = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .format("csv") \
        .load(store_path)
    
    store_count = df_store_master.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Loaded {store_count:,} stores")
    log_metrics("read_store_master", store_count, duration)
    
except Exception as e:
    print(f"✗ Error reading store master: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Read Retail Calendar
# M002: Load retail calendar (CSV)
start_time = datetime.now()

calendar_path = f"{input_path}/reference/retail_calendar.csv"
print(f"Reading retail calendar from: {calendar_path}")

try:
    df_retail_calendar = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .format("csv") \
        .load(calendar_path)
    
    calendar_count = df_retail_calendar.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Loaded {calendar_count:,} calendar records")
    log_metrics("read_retail_calendar", calendar_count, duration)
    
except Exception as e:
    print(f"✗ Error reading retail calendar: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Display Master Data Samples
# Display master data summary
print("\n" + "="*70)
print("MASTER DATA SUMMARY")
print("="*70)
print(f"Product Master:       {df_product_master.count():,} records (broadcast-optimized)")
print(f"Customer Master:      {df_customer_master.count():,} records (cached)")
print(f"Store Master:         {df_store_master.count():,} records")
print(f"Retail Calendar:      {df_retail_calendar.count():,} records")
print("="*70)

print("\n[Product Master Sample]")
display(df_product_master.limit(5))

print("\n[Customer Master Sample]")
display(df_customer_master.limit(5))

print("\n[Store Master Sample]")
display(df_store_master.limit(5))

print("\n[Retail Calendar Sample]")
display(df_retail_calendar.limit(5))

# COMMAND ----------


