# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,E-commerce Bronze Ingestion
# MAGIC %md
# MAGIC # 11_write_bronze_ecommerce: E-commerce Data Ingestion to Bronze
# MAGIC
# MAGIC This notebook ingests e-commerce transaction data to Bronze layer:
# MAGIC * **online_orders_YYYYMMDD.json** → `bronze.online_orders`
# MAGIC
# MAGIC **Source Path:** `s3://.../raw/ecommerce/online_orders_YYYYMMDD.json`
# MAGIC
# MAGIC **Bronze Layer Characteristics:**
# MAGIC * All columns stored as STRING (schema-on-read)
# MAGIC * No validation - accepts all data as-is
# MAGIC * Append-only mode (transactional data)
# MAGIC * Partitioned by `_job_date` for efficient querying
# MAGIC * Metadata enriched with ingestion timestamp and batch ID
# MAGIC
# MAGIC **Dependencies:** Run `00_initialize` first

# COMMAND ----------

# DBTITLE 1,Run Initialization
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Ingest Online Orders
# Ingest online_orders_YYYYMMDD.json to Bronze
# ↓ Step 1: Source Data Extraction
start_time = datetime.now()

# Build file path with job_date pattern
file_name = f"online_orders_{job_date.replace('-', '')}.json"
file_path = f"{input_path}/ecommerce/{file_name}"
bronze_table = f"{catalog_name}.bronze.online_orders"

print(f"Reading e-commerce orders from: {file_path}")
print(f"Target table: {bronze_table}")

try:
    # Read JSON with all columns as STRING (schema-on-read)
    df_raw = spark.read \
        .option("inferSchema", "false") \
        .option("mode", "PERMISSIVE") \
        .json(file_path)
    
    print(f"✓ Raw records read: {df_raw.count():,}")
    
except Exception as e:
    error_code = "E002"
    error_msg = f"Failed to read e-commerce file: {str(e)}"
    print(f"✗ {error_code}: {error_msg}")
    raise Exception(f"{error_code}: {error_msg}")

# COMMAND ----------

# DBTITLE 1,Enrich with Bronze Metadata
# ↓ Step 2: Enrich with Bronze metadata
print("\nEnriching with bronze metadata...")

df_bronze = df_raw \
    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_source_file", F.lit(file_path)) \
    .withColumn("_batch_id", F.lit(batch_id)) \
    .withColumn("_job_date", F.lit(job_date))

print(f"✓ Metadata columns added: _ingestion_timestamp, _source_file, _batch_id, _job_date")
print(f"  Total columns: {len(df_bronze.columns)}")

# COMMAND ----------

# DBTITLE 1,Write to Bronze Delta Lake
# ↓ Step 3: Persist to Bronze Delta Lake
# Write to Bronze layer (append-only, partitioned by _job_date)
start_write = datetime.now()

print(f"\nWriting to Bronze table: {bronze_table}")

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("_job_date") \
    .saveAsTable(bronze_table)

write_duration = (datetime.now() - start_write).total_seconds()
total_duration = (datetime.now() - start_time).total_seconds()

record_count = df_bronze.count()

print(f"✓ Successfully wrote {record_count:,} records to Bronze")
print(f"  Write duration: {write_duration:.2f} seconds")
print(f"  Total duration: {total_duration:.2f} seconds")

log_metrics("bronze_online_orders", record_count, total_duration)

# COMMAND ----------

# DBTITLE 1,E-commerce Bronze Summary
# ✓ Complete: Bronze Ingestion
# Display ingestion summary
print("\n" + "="*70)
print("E-COMMERCE BRONZE INGESTION COMPLETE")
print("="*70)
print(f"Batch ID:        {batch_id}")
print(f"Job Date:        {job_date}")
print(f"Source File:     {file_path}")
print(f"\nIngested Table:")
print(f"  ✓ {bronze_table}")
print(f"  Records:       {record_count:,}")
print(f"  Partition:     _job_date={job_date}")
print("="*70)
print("\n✓ E-commerce data successfully landed in Bronze layer")
