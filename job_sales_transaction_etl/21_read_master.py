# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M002: Load Master Tables
# MAGIC %md
# MAGIC # 02_read_master: Load Master Tables from Bronze (M003)
# MAGIC
# MAGIC This notebook reads master/reference data from the Bronze layer:
# MAGIC * **bronze.customer_master** - Customer dimension
# MAGIC * **bronze.product_master** - Product dimension (broadcast-optimized)
# MAGIC * **bronze.store_master** - Store dimension
# MAGIC * **bronze.retail_calendar** - Time dimension
# MAGIC
# MAGIC **Data Flow:** Bronze Delta Tables → Typed DataFrames
# MAGIC
# MAGIC **Dependencies:** Run `00_initialize` and bronze ingestion notebooks first
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
# M003: Load product master from Bronze - Optimized for Broadcast joins
start_time = datetime.now()

bronze_table = f"{catalog_name}.bronze.product_master"
print(f"Reading product master from Bronze: {bronze_table}")

try:
    # Read from Bronze Delta table with schema casting
    df_product_master = spark.table(bronze_table) \
        .select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("category_id"),
            F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
            F.col("cost_price").cast("decimal(18,2)").alias("cost_price"),
            F.col("supplier_id")
        )
    
    # Optimize for broadcast joins (product master is typically small)
    df_product_master = df_product_master.hint("broadcast")
    
    product_count = df_product_master.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Loaded {product_count:,} products from Bronze (broadcast-optimized)")
    log_metrics("read_product_master_bronze", product_count, duration)
    
except Exception as e:
    print(f"✗ Error reading product master from Bronze: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Read Customer Master
# M003: Load customer master from Bronze
start_time = datetime.now()

bronze_table = f"{catalog_name}.bronze.customer_master"
print(f"Reading customer master from Bronze: {bronze_table}")

try:
    df_customer_master = spark.table(bronze_table) \
        .select(
            F.col("customer_id"),
            F.col("customer_name"),
            F.col("email"),
            F.col("phone"),
            F.col("birth_date").cast("date").alias("birth_date"),
            F.col("gender"),
            F.col("registration_date").cast("date").alias("registration_date")
        )
    
    customer_count = df_customer_master.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Loaded {customer_count:,} customers from Bronze")
    log_metrics("read_customer_master_bronze", customer_count, duration)
    
except Exception as e:
    print(f"✗ Error reading customer master from Bronze: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Read Store Master
# M003: Load store master from Bronze
start_time = datetime.now()

bronze_table = f"{catalog_name}.bronze.store_master"
print(f"Reading store master from Bronze: {bronze_table}")

try:
    df_store_master = spark.table(bronze_table) \
        .select(
            F.col("store_id"),
            F.col("store_name"),
            F.col("region"),
            F.col("open_date").cast("date").alias("open_date"),
            F.col("store_type"),
            F.col("manager_name")
        )
    
    store_count = df_store_master.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Loaded {store_count:,} stores from Bronze")
    log_metrics("read_store_master_bronze", store_count, duration)
    
except Exception as e:
    print(f"✗ Error reading store master from Bronze: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Read Retail Calendar
# M003: Load retail calendar from Bronze
start_time = datetime.now()

bronze_table = f"{catalog_name}.bronze.retail_calendar"
print(f"Reading retail calendar from Bronze: {bronze_table}")

try:
    df_retail_calendar = spark.table(bronze_table) \
        .select(
            F.col("date").cast("date").alias("date"),
            F.col("year").cast("int").alias("year"),
            F.col("month").cast("int").alias("month"),
            F.col("day").cast("int").alias("day"),
            F.col("day_of_week"),
            F.col("fiscal_year").cast("int").alias("fiscal_year"),
            F.col("fiscal_quarter"),
            F.col("is_retail_peak").cast("boolean").alias("is_retail_peak")
        )
    
    calendar_count = df_retail_calendar.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Loaded {calendar_count:,} calendar records from Bronze")
    log_metrics("read_retail_calendar_bronze", calendar_count, duration)
    
except Exception as e:
    print(f"✗ Error reading retail calendar from Bronze: {str(e)}")
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


