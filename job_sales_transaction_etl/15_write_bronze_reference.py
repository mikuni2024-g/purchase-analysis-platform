# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Reference Bronze Ingestion
# MAGIC %md
# MAGIC # 15_write_bronze_reference: Reference Data Ingestion to Bronze
# MAGIC
# MAGIC This notebook ingests reference/dimension data sources to Bronze layer:
# MAGIC * **retail_calendar.csv** → `bronze.retail_calendar`
# MAGIC * **holiday_calendar.csv** → `bronze.holiday_calendar`
# MAGIC
# MAGIC **Bronze Layer Characteristics:**
# MAGIC * All columns stored as STRING (schema-on-read)
# MAGIC * No validation - accepts all data as-is
# MAGIC * Overwrite mode (full refresh for reference data)
# MAGIC * Metadata enriched with ingestion timestamp and batch ID
# MAGIC
# MAGIC **Dependencies:** Run `00_initialize` first

# COMMAND ----------

# DBTITLE 1,Run Initialization
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Ingest Retail Calendar
# Ingest retail_calendar.csv to Bronze
start_time = datetime.now()

file_path = f"{input_path}/reference/retail_calendar.csv"
bronze_table = f"{catalog_name}.bronze.retail_calendar"

print(f"Reading retail calendar from: {file_path}")

try:
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(file_path)
    
    df_bronze = df_raw \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(file_path)) \
        .withColumn("_batch_id", F.lit(batch_id))
    
    df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table)
    
    record_count = df_bronze.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Ingested {record_count:,} retail calendar records")
    log_metrics("bronze_retail_calendar", record_count, duration)
    
except Exception as e:
    print(f"✗ Error: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Ingest Holiday Calendar
# Ingest holiday_calendar.csv to Bronze
start_time = datetime.now()

file_path = f"{input_path}/reference/holiday_calendar.csv"
bronze_table = f"{catalog_name}.bronze.holiday_calendar"

print(f"\nReading holiday calendar from: {file_path}")

try:
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(file_path)
    
    df_bronze = df_raw \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(file_path)) \
        .withColumn("_batch_id", F.lit(batch_id))
    
    df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table)
    
    record_count = df_bronze.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Ingested {record_count:,} holiday calendar records")
    log_metrics("bronze_holiday_calendar", record_count, duration)
    
except Exception as e:
    print(f"✗ Error: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Reference Bronze Summary
# Display Reference Bronze ingestion summary
print("\n" + "="*70)
print("REFERENCE BRONZE INGESTION COMPLETE")
print("="*70)
print(f"Batch ID:        {batch_id}")
print(f"Job Date:        {job_date}")
print("\nIngested Tables:")
print(f"  ✓ {catalog_name}.bronze.retail_calendar")
print(f"  ✓ {catalog_name}.bronze.holiday_calendar")
print("="*70)
print("\n✓ Reference data successfully landed in Bronze layer")
