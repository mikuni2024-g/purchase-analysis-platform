# Databricks notebook source
# DBTITLE 1,ERP Bronze Ingestion
# MAGIC %md
# MAGIC # 01_write_bronze_erp: ERP Data Ingestion to Bronze
# MAGIC
# MAGIC This notebook ingests ERP (Enterprise Resource Planning) data sources to Bronze layer:
# MAGIC * **product_master.csv** → `bronze.product_master`
# MAGIC * **category_master.csv** → `bronze.category_master`
# MAGIC * **supplier_master.csv** → `bronze.supplier_master`
# MAGIC * **store_master.csv** → `bronze.store_master`
# MAGIC * **inventory_balance.parquet** → `bronze.inventory_balance`
# MAGIC
# MAGIC **Bronze Layer Characteristics:**
# MAGIC * All columns stored as STRING (schema-on-read)
# MAGIC * No validation - accepts all data as-is
# MAGIC * Master tables: overwrite mode (full refresh)
# MAGIC * Metadata enriched with ingestion timestamp and batch ID
# MAGIC
# MAGIC **Dependencies:** Run `00_initialize` first

# COMMAND ----------

# DBTITLE 1,Run Initialization
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Ingest Product Master
# Ingest product_master.csv to Bronze
start_time = datetime.now()

file_path = f"{input_path}/erp/product_master.csv"
bronze_table = f"{catalog_name}.bronze.product_master"

print(f"Reading ERP product master from: {file_path}")

try:
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(file_path)
    
    df_bronze = df_raw \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(file_path)) \
        .withColumn("_batch_id", F.lit(batch_id))
    
    df_bronze.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    
    record_count = df_bronze.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Ingested {record_count:,} product records")
    log_metrics("bronze_product_master", record_count, duration)
    
except Exception as e:
    print(f"✗ Error: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Ingest Category Master
# Ingest category_master.csv to Bronze
start_time = datetime.now()

file_path = f"{input_path}/erp/category_master.csv"
bronze_table = f"{catalog_name}.bronze.category_master"

print(f"\nReading category master from: {file_path}")

try:
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(file_path)
    
    df_bronze = df_raw \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(file_path)) \
        .withColumn("_batch_id", F.lit(batch_id))
    
    df_bronze.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    
    record_count = df_bronze.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Ingested {record_count:,} category records")
    log_metrics("bronze_category_master", record_count, duration)
    
except Exception as e:
    print(f"✗ Error: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Ingest Supplier Master
# Ingest supplier_master.csv to Bronze
start_time = datetime.now()

file_path = f"{input_path}/erp/supplier_master.csv"
bronze_table = f"{catalog_name}.bronze.supplier_master"

print(f"\nReading supplier master from: {file_path}")

try:
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(file_path)
    
    df_bronze = df_raw \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(file_path)) \
        .withColumn("_batch_id", F.lit(batch_id))
    
    df_bronze.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    
    record_count = df_bronze.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Ingested {record_count:,} supplier records")
    log_metrics("bronze_supplier_master", record_count, duration)
    
except Exception as e:
    print(f"✗ Error: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Ingest Store Master
# Ingest store_master.csv to Bronze
start_time = datetime.now()

file_path = f"{input_path}/erp/store_master.csv"
bronze_table = f"{catalog_name}.bronze.store_master"

print(f"\nReading store master from: {file_path}")

try:
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(file_path)
    
    df_bronze = df_raw \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(file_path)) \
        .withColumn("_batch_id", F.lit(batch_id))
    
    df_bronze.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    
    record_count = df_bronze.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Ingested {record_count:,} store records")
    log_metrics("bronze_store_master", record_count, duration)
    
except Exception as e:
    print(f"✗ Error: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Ingest Inventory Balance
# Ingest inventory_balance.parquet to Bronze
start_time = datetime.now()

file_path = f"{input_path}/erp/inventory_balance.parquet"
bronze_table = f"{catalog_name}.bronze.inventory_balance"

print(f"\nReading inventory balance from: {file_path}")

try:
    df_raw = spark.read.parquet(file_path)
    
    # Cast all columns to STRING for Bronze layer
    for col_name in df_raw.columns:
        df_raw = df_raw.withColumn(col_name, F.col(col_name).cast("string"))
    
    df_bronze = df_raw \
        .withColumn("_ingestion_timestamp", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(file_path)) \
        .withColumn("_batch_id", F.lit(batch_id))
    
    df_bronze.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    
    record_count = df_bronze.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✓ Ingested {record_count:,} inventory records")
    log_metrics("bronze_inventory_balance", record_count, duration)
    
except Exception as e:
    print(f"✗ Error: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,ERP Bronze Summary
# Display ERP Bronze ingestion summary
print("\n" + "="*70)
print("ERP BRONZE INGESTION COMPLETE")
print("="*70)
print(f"Batch ID:        {batch_id}")
print(f"Job Date:        {job_date}")
print("\nIngested Tables:")
print(f"  ✓ {catalog_name}.bronze.product_master")
print(f"  ✓ {catalog_name}.bronze.category_master")
print(f"  ✓ {catalog_name}.bronze.supplier_master")
print(f"  ✓ {catalog_name}.bronze.store_master")
print(f"  ✓ {catalog_name}.bronze.inventory_balance")
print("="*70)
print("\n✓ ERP data successfully landed in Bronze layer")

# COMMAND ----------


