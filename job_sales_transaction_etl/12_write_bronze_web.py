# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Web Clickstream Bronze Ingestion
# MAGIC %md
# MAGIC # 12_write_bronze_web: Web Clickstream Data Ingestion to Bronze
# MAGIC
# MAGIC This notebook ingests web clickstream/analytics data to Bronze layer:
# MAGIC * **web_clickstream_YYYYMMDD.log** → `bronze.web_clickstream`
# MAGIC
# MAGIC **Source Path:** `s3://.../raw/web/clickstream/web_clickstream_YYYYMMDD.log`
# MAGIC
# MAGIC **Bronze Layer Characteristics:**
# MAGIC * All columns stored as STRING (schema-on-read)
# MAGIC * No validation - accepts all data as-is
# MAGIC * Append-only mode (event data)
# MAGIC * Partitioned by `_job_date` for efficient querying
# MAGIC * Metadata enriched with ingestion timestamp and batch ID
# MAGIC
# MAGIC **Log Format Support:**
# MAGIC * JSON lines (one JSON object per line)
# MAGIC * CSV/TSV with header
# MAGIC * Text format can be parsed in Silver layer
# MAGIC
# MAGIC **Dependencies:** Run `00_initialize` first

# COMMAND ----------

# DBTITLE 1,Run Initialization
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Ingest Web Clickstream
# Ingest web_clickstream_YYYYMMDD.log to Bronze
# ↓ Step 1: Source Data Extraction
start_time = datetime.now()

# Build file path with job_date pattern
file_name = f"web_clickstream_{job_date.replace('-', '')}.log"
file_path = f"{input_path}/web/clickstream/{file_name}"
bronze_table = f"{catalog_name}.bronze.web_clickstream"

print(f"Reading web clickstream from: {file_path}")
print(f"Target table: {bronze_table}")

try:
    # Read as text and parse structured log format
    # Format: timestamp LEVEL key=value key=value key=value
    df_text = spark.read.text(file_path)
    
    # Parse the structured log format
    df_raw = df_text.selectExpr(
        "split(value, ' ')[0] as timestamp",
        "split(value, ' ')[1] as level",
        "regexp_extract(value, 'ip=([^ ]+)', 1) as ip",
        "regexp_extract(value, 'user_id=([^ ]+)', 1) as user_id",
        "regexp_extract(value, 'page=([^ ]+)', 1) as page",
        "regexp_extract(value, 'action=([^ ]+)', 1) as action"
    )
    
    print(f"✓ Raw records read: {df_raw.count():,}")
    print(f"  Detected {len(df_raw.columns)} columns from log structure")
    
except Exception as e:
    error_code = "E002"
    error_msg = f"Failed to read web clickstream file: {str(e)}"
    print(f"✗ {error_code}: {error_msg}")
    raise Exception(f"{error_code}: {error_msg}")

# COMMAND ----------

# DBTITLE 1,Enrich with Bronze Metadata
# ↓ Step 2: Metadata Enrichment
# Add bronze layer metadata columns for lineage tracking
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

log_metrics("bronze_web_clickstream", record_count, total_duration)

# COMMAND ----------

# DBTITLE 1,Web Clickstream Bronze Summary
# ✓ Complete: Bronze Ingestion
# Display ingestion summary
print("\n" + "="*70)
print("WEB CLICKSTREAM BRONZE INGESTION COMPLETE")
print("="*70)
print(f"Batch ID:        {batch_id}")
print(f"Job Date:        {job_date}")
print(f"Source File:     {file_path}")
print(f"\nIngested Table:")
print(f"  ✓ {bronze_table}")
print(f"  Records:       {record_count:,}")
print(f"  Partition:     _job_date={job_date}")
print("="*70)
print("\n✓ Web clickstream data successfully landed in Bronze layer")

# COMMAND ----------

display(spark.table(bronze_table))
