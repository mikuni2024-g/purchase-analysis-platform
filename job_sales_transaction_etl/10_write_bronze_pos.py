# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M001: Write to Bronze Layer
# MAGIC %md
# MAGIC # 01_write_bronze: Raw Data Ingestion to Bronze (M001)
# MAGIC
# MAGIC This notebook implements the **Bronze layer** - the raw data landing zone:
# MAGIC * Reads POS transaction data from S3 (CSV format)
# MAGIC * Preserves data exactly as received with minimal processing
# MAGIC * Adds metadata columns for lineage tracking
# MAGIC * Writes to Delta Lake for ACID guarantees and time travel
# MAGIC
# MAGIC **Bronze Layer Characteristics:**
# MAGIC * **Schema-on-read** - All business columns stored as STRING to accept any data
# MAGIC * **No validation** - Accepts all records, even malformed ones
# MAGIC * **Append-only** - Immutable raw data history
# MAGIC * **Metadata enriched** - Tracks ingestion timestamp, source file, batch ID
# MAGIC
# MAGIC **Dependencies:** Run `00_initialize` first
# MAGIC
# MAGIC **Output:** `bronze.pos_transactions` table

# COMMAND ----------

# DBTITLE 1,Run Initialization
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Read Raw POS Transactions from S3
# M001: Read raw POS transaction data from S3
start_time = datetime.now()

# Construct S3 file path (date format: YYYYMMDD without dashes)
file_date = job_date.replace("-", "")
file_path = f"{input_path}/pos/transactions/pos_transactions_{file_date}.csv"

print(f"Reading raw data from S3: {file_path}")

try:
    # Read CSV with ALL columns as STRING (schema-on-read approach)
    # This ensures we accept any data format and preserve it exactly as-is
    # Handles bad rows gracefully ↓
    # .option("mode", "PERMISSIVE")

    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("mode", "PERMISSIVE") \
        .csv(file_path)
    
    # Check if file exists and has data
    if df_raw.count() == 0:
        error_msg = f"E002: Source file is empty: {file_path}"
        print(f"✗ {error_msg}")
        raise ValueError(error_msg)
    
    raw_count = df_raw.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Read {raw_count:,} raw records from S3")
    print(f"  Duration: {duration:.2f} seconds")
    
    log_metrics("read_raw_s3", raw_count, duration)
    
except Exception as e:
    if "Path does not exist" in str(e) or "FileNotFoundException" in str(e):
        error_msg = f"E002: Source file not found: {file_path}"
        print(f"✗ {error_msg}")
        raise FileNotFoundError(error_msg)
    else:
        print(f"✗ Error reading from S3: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Add Bronze Metadata Columns
# Add bronze layer metadata columns for lineage tracking
print("\nEnriching with bronze metadata...")

df_bronze = df_raw \
    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_source_file", F.lit(file_path)) \
    .withColumn("_batch_id", F.lit(batch_id)) \
    .withColumn("_job_date", F.lit(job_date))

print(f"✓ Added metadata columns:")
print(f"  - _ingestion_timestamp: Current timestamp")
print(f"  - _source_file: {file_path}")
print(f"  - _batch_id: {batch_id}")
print(f"  - _job_date: {job_date}")

# COMMAND ----------

# DBTITLE 1,Display Sample Data
# Display sample of bronze data before writing
print("\n" + "="*70)
print("BRONZE LAYER SAMPLE (First 5 records)")
print("="*70)
display(df_bronze.limit(5))

# COMMAND ----------

# DBTITLE 1,Write to Bronze Delta Table
# Write to Bronze layer (append-only, no modifications)
start_write = datetime.now()
bronze_table = f"{catalog_name}.bronze.pos_transactions"

print(f"\nWriting to Bronze layer: {bronze_table}")

try:
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("_job_date") \
        .saveAsTable(bronze_table)
    
    write_count = df_bronze.count()
    write_duration = (datetime.now() - start_write).total_seconds()
    total_duration = (datetime.now() - start_time).total_seconds()
    
    log_metrics("write_bronze", write_count, write_duration)
    
    print(f"\n✓ Successfully wrote {write_count:,} records to Bronze")
    print(f"  Write duration: {write_duration:.2f} seconds")
    print(f"  Total duration: {total_duration:.2f} seconds")
    print(f"  Partition: _job_date={job_date}")
    
except Exception as e:
    print(f"\n✗ Error writing to Bronze: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Bronze Ingestion Summary
# Display ingestion summary
print("\n" + "="*70)
print("BRONZE INGESTION COMPLETE")
print("="*70)
print(f"Source:          {file_path}")
print(f"Target:          {bronze_table}")
print(f"Records:         {df_bronze.count():,}")
print(f"Batch ID:        {batch_id}")
print(f"Job Date:        {job_date}")
print(f"Partition:       _job_date={job_date}")
print("="*70)
print("\n✓ Raw data successfully landed in Bronze layer")
print("  Next: Run 02_read_sales to read from Bronze for validation & transformation")
