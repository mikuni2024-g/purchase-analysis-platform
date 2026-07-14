# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M007: Audit Logging
# MAGIC %md
# MAGIC # 07_audit_log: Job Audit Logging (M007)
# MAGIC
# MAGIC This notebook handles:
# MAGIC * Recording job execution metadata
# MAGIC * Logging processing metrics
# MAGIC * Capturing success/failure status
# MAGIC * Creating audit trail
# MAGIC * Storing execution history
# MAGIC
# MAGIC **Dependencies:** Run all previous notebooks first
# MAGIC
# MAGIC **Output:** Audit log table with job execution details

# COMMAND ----------

# DBTITLE 1,Run Initialization
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Define Audit Log Schema
# Define audit log schema
audit_log_schema = StructType([
    StructField("batch_id", StringType(), False),
    StructField("job_date", StringType(), False),
    StructField("job_name", StringType(), False),
    StructField("environment", StringType(), False),
    StructField("start_timestamp", TimestampType(), False),
    StructField("end_timestamp", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("records_processed", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("execution_duration_seconds", DoubleType(), True)
])

print("✓ Audit log schema defined")

# COMMAND ----------

# DBTITLE 1,Collect Job Metrics
# Collect job execution metrics
start_time = datetime.now()

print("Collecting job metrics...")

# Get target table statistics
target_table = f"{catalog_name}.{schema_name}.sales_transactions_silver"

try:
    records_processed = spark.table(target_table) \
        .filter(F.col("batch_id") == batch_id) \
        .count()
    
    job_status = "SUCCESS"
    error_message = None
    
except Exception as e:
    records_processed = 0
    job_status = "FAILED"
    error_message = str(e)

end_time = datetime.now()
execution_duration = (end_time - start_time).total_seconds()

print(f"✓ Job metrics collected")
print(f"  Status: {job_status}")
print(f"  Records Processed: {records_processed:,}")

# COMMAND ----------

# DBTITLE 1,Create Audit Log Entry
# Create audit log entry
audit_entry = spark.createDataFrame([
    {
        "batch_id": batch_id,
        "job_date": job_date,
        "job_name": "sales_transaction_etl",
        "environment": environment,
        "start_timestamp": start_time,
        "end_timestamp": end_time,
        "status": job_status,
        "records_processed": records_processed,
        "error_message": error_message,
        "execution_duration_seconds": execution_duration
    }
], schema=audit_log_schema)

print("✓ Audit log entry created")

# COMMAND ----------

# DBTITLE 1,Write Audit Log
# Write audit log to table
audit_table = f"{catalog_name}.{schema_name}.job_audit_log"

print(f"\nWriting audit log to: {audit_table}")

try:
    audit_entry.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(audit_table)
    
    print("✓ Audit log written successfully")
    
except Exception as e:
    print(f"✗ Error writing audit log: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Display Job Summary
# Display final job summary
print("\n" + "="*70)
print("JOB EXECUTION SUMMARY")
print("="*70)
print(f"Batch ID:          {batch_id}")
print(f"Job Date:          {job_date}")
print(f"Environment:       {environment}")
print(f"Status:            {job_status}")
print(f"Records Processed: {records_processed:,}")
print(f"Start Time:        {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"End Time:          {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Duration:          {execution_duration:.2f} seconds")
if error_message:
    print(f"Error:             {error_message}")
print("="*70)

# Show recent audit logs
print("\nRecent Audit Logs:")
display(spark.table(audit_table).orderBy(F.col("start_timestamp").desc()).limit(10))
