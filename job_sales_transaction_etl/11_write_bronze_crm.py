# Databricks notebook source
# DBTITLE 1,CRM Bronze Ingestion
# MAGIC %md
# MAGIC # 01_write_bronze_crm: CRM Data Ingestion to Bronze
# MAGIC
# MAGIC This notebook ingests CRM (Customer Relationship Management) data sources to Bronze layer:
# MAGIC * **customer_master.json** → `bronze.customer_master`
# MAGIC * **loyalty_members.csv** → `bronze.loyalty_members`
# MAGIC * **coupon_dispatches.json** → `bronze.coupon_dispatches`
# MAGIC
# MAGIC **Bronze Layer Characteristics:**
# MAGIC * All columns stored as STRING (schema-on-read)
# MAGIC * No validation - accepts all data as-is
# MAGIC * Append-only mode (except master tables use overwrite)
# MAGIC * Metadata enriched with ingestion timestamp and batch ID
# MAGIC
# MAGIC **Dependencies:** Run `00_initialize` first

# COMMAND ----------

# DBTITLE 1,Run Initialization
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Ingest Customer Master
# Ingest customer_master.json to Bronze
start_time = datetime.now()

file_path = f"{input_path}/crm/customer_master.json"
bronze_table = f"{catalog_name}.bronze.customer_master"

print(f"Reading CRM customer master from: {file_path}")

try:
    # Read JSON with all columns as STRING
    df_raw = spark.read \
        .option("inferSchema", "false") \
        .option("mode", "PERMISSIVE") \
        .json(file_path)
    
    # Add bronze metadata
    df_bronze = df_raw \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(file_path)) \
        .withColumn("_batch_id", F.lit(batch_id))
    
    # Write to Bronze (overwrite for master table)
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(bronze_table)
    
    record_count = df_bronze.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Ingested {record_count:,} customer records to Bronze")
    print(f"  Target: {bronze_table}")
    print(f"  Duration: {duration:.2f} seconds")
    
    log_metrics("bronze_customer_master", record_count, duration)
    
except Exception as e:
    print(f"✗ Error ingesting customer master: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Ingest Loyalty Members
# Ingest loyalty_members.csv to Bronze
start_time = datetime.now()

file_path = f"{input_path}/crm/loyalty_members.csv"
bronze_table = f"{catalog_name}.bronze.loyalty_members"

print(f"\nReading loyalty members from: {file_path}")

try:
    # Read CSV with all columns as STRING
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("mode", "PERMISSIVE") \
        .csv(file_path)
    
    # Add bronze metadata
    df_bronze = df_raw \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(file_path)) \
        .withColumn("_batch_id", F.lit(batch_id))
    
    # Write to Bronze (overwrite for master table)
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(bronze_table)
    
    record_count = df_bronze.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Ingested {record_count:,} loyalty member records to Bronze")
    print(f"  Target: {bronze_table}")
    print(f"  Duration: {duration:.2f} seconds")
    
    log_metrics("bronze_loyalty_members", record_count, duration)
    
except Exception as e:
    print(f"✗ Error ingesting loyalty members: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Ingest Coupon Dispatches
# Ingest coupon_dispatches.json to Bronze
start_time = datetime.now()

file_path = f"{input_path}/crm/coupon_dispatches.json"
bronze_table = f"{catalog_name}.bronze.coupon_dispatches"

print(f"\nReading coupon dispatches from: {file_path}")

try:
    # Read JSON with all columns as STRING
    df_raw = spark.read \
        .option("inferSchema", "false") \
        .option("mode", "PERMISSIVE") \
        .json(file_path)
    
    # Add bronze metadata
    df_bronze = df_raw \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(file_path)) \
        .withColumn("_batch_id", F.lit(batch_id))
    
    # Write to Bronze (overwrite for snapshot table)
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(bronze_table)
    
    record_count = df_bronze.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Ingested {record_count:,} coupon dispatch records to Bronze")
    print(f"  Target: {bronze_table}")
    print(f"  Duration: {duration:.2f} seconds")
    
    log_metrics("bronze_coupon_dispatches", record_count, duration)
    
except Exception as e:
    print(f"✗ Error ingesting coupon dispatches: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,CRM Bronze Summary
# Display CRM Bronze ingestion summary
print("\n" + "="*70)
print("CRM BRONZE INGESTION COMPLETE")
print("="*70)
print(f"Batch ID:        {batch_id}")
print(f"Job Date:        {job_date}")
print("\nIngested Tables:")
print(f"  ✓ {catalog_name}.bronze.customer_master")
print(f"  ✓ {catalog_name}.bronze.loyalty_members")
print(f"  ✓ {catalog_name}.bronze.coupon_dispatches")
print("="*70)
print("\n✓ CRM data successfully landed in Bronze layer")

# COMMAND ----------


