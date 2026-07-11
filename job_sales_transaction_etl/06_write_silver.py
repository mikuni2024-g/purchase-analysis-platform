# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M006: Write to Silver Layer
# MAGIC %md
# MAGIC # 06_write_silver: Persistence to Delta Lake (M006)
# MAGIC
# MAGIC This notebook handles:
# MAGIC * Writing enriched data to Delta Lake
# MAGIC * Adding audit columns
# MAGIC * Partitioning strategy
# MAGIC * Table optimization
# MAGIC * Creating/updating silver layer tables
# MAGIC
# MAGIC **Dependencies:** Run `05_join_master` first
# MAGIC
# MAGIC **Output:** Delta table in silver layer

# COMMAND ----------

# DBTITLE 1,Run Prerequisites
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Load Enriched Data
# MAGIC %run ./05_join_master

# COMMAND ----------

# DBTITLE 1,Add Audit Columns
# Add audit columns to enriched data
start_time = datetime.now()

print("Adding audit columns...")

df_silver = add_audit_columns(df_sales_enriched)

print(f"✓ Audit columns added: {df_silver.count():,} records")

# COMMAND ----------

# DBTITLE 1,Define Target Table
# Define target table name
target_table = f"{catalog_name}.{schema_name}.sales_transactions_silver"

print(f"Target table: {target_table}")

# COMMAND ----------

# DBTITLE 1,Write to Delta Lake
# Write data to Delta Lake
print(f"\nWriting to Delta Lake: {target_table}")

try:
    # Ensure schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
    
    # Write with partitioning by job_date
    df_silver.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("job_date") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
    
    write_count = df_silver.count()
    duration = (datetime.now() - start_time).total_seconds()
    
    log_metrics("write_silver", write_count, duration)
    
    print(f"\n✓ Successfully wrote {write_count:,} records")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"  Partition: job_date={job_date}")
    
except Exception as e:
    print(f"\n✗ Error writing to Delta Lake: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Optimize Table
# Optimize the Delta table
print("\nOptimizing Delta table...")

try:
    spark.sql(f"OPTIMIZE {target_table} WHERE job_date = '{job_date}'")
    print("✓ Table optimized")
except Exception as e:
    print(f"⚠️  Optimization warning: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Display Table Statistics
# Display table statistics
print("\n" + "="*60)
print("DELTA TABLE STATISTICS")
print("="*60)

# Get table details
table_info = spark.sql(f"DESCRIBE DETAIL {target_table}").collect()[0]

print(f"Table:           {target_table}")
print(f"Format:          {table_info['format']}")
print(f"Location:        {table_info['location']}")
print(f"Partition Cols:  {table_info['partitionColumns']}")
print("="*60)

# Show sample of written data
print("\nSample of written data:")
display(spark.table(target_table).filter(F.col("job_date") == job_date).limit(10))
