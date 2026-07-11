# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M001: Load POS Transactions
# MAGIC %md
# MAGIC # 01_read_sales: Load POS Transactions (M001)
# MAGIC
# MAGIC This notebook implements POS sales ingestion with strict schema enforcement:
# MAGIC * Reads POS transactions from S3: `pos/transactions/pos_transactions_{job_date}.csv`
# MAGIC * **E002 Error:** Throws FileNotFoundError if file is missing
# MAGIC * **E003 Error:** Throws ValueError on schema mismatch
# MAGIC * Applies strict schema casting for all fields
# MAGIC * FAILFAST mode for immediate error detection
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
# M001: POS Sales Ingestion with Schema Enforcement
start_time = datetime.now()

# Define strict schema for POS transactions
pos_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(10, 2), False),
    StructField("discount_amount", DecimalType(10, 2), False),
    StructField("transaction_time", TimestampType(), False)
])

# Construct the POS sales data path
# Convert job_date from YYYY-MM-DD to YYYYMMDD format to match S3 file naming
date_for_filename = job_date.replace("-", "")
sales_path = f"{input_path}/pos/transactions/pos_transactions_{date_for_filename}.csv"

print(f"Reading POS transactions from: {sales_path}")

# E002: Check if file exists
try:
    file_exists = len(dbutils.fs.ls(sales_path)) > 0
except Exception:
    error_message = f"E002: POS transaction file not found: {sales_path}"
    print(f"\n✗ {error_message}")
    raise FileNotFoundError(error_message)

print("✓ File exists, proceeding with read...")

# Read with strict schema enforcement
try:
    df_sales_raw = spark.read \
        .option("header", "true") \
        .option("mode", "FAILFAST") \
        .schema(pos_schema) \
        .csv(sales_path)
    
    record_count = df_sales_raw.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Loaded {record_count:,} POS transactions")
    log_metrics("read_pos_sales", record_count, duration)
    
except Exception as e:
    # E003: Schema mismatch error
    if "schema" in str(e).lower() or "type" in str(e).lower() or "cannot cast" in str(e).lower():
        error_message = f"E003: Schema mismatch in POS transaction file: {sales_path}\nDetails: {str(e)}"
        print(f"\n✗ {error_message}")
        raise ValueError(error_message)
    else:
        print(f"✗ Error reading POS sales data: {str(e)}")
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
