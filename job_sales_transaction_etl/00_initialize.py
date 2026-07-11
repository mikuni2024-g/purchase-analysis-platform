# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC # 00_initialize: Setup & Parameter Handling
# MAGIC
# MAGIC This notebook initializes the ETL job by:
# MAGIC * Defining and validating job parameters
# MAGIC * Verifying AWS S3 connectivity (E001 error handling)
# MAGIC * Setting up Unity Catalog schemas and tables
# MAGIC * Preparing the execution environment
# MAGIC * Providing reusable utility functions
# MAGIC
# MAGIC **Job Parameters:**
# MAGIC * `job_date` - Processing date (YYYY-MM-DD)
# MAGIC * `environment` - Deployment environment (dev/test/prod)
# MAGIC * `input_path` - Source data location (S3 path)
# MAGIC * `output_database` - Target database (catalog.schema)
# MAGIC * `batch_id` - Unique batch identifier
# MAGIC
# MAGIC **Target Tables:**
# MAGIC * `silver.fact_sales` - Validated and enriched sales transactions
# MAGIC * `silver.reject_sales` - Rejected records with rejection reasons
# MAGIC * `audit.etl_job_log` - Job execution audit trail

# COMMAND ----------

# DBTITLE 1,Import Libraries
# Import required libraries
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import uuid

# COMMAND ----------

# DBTITLE 1,Define Job Parameters
# Define job parameters using Databricks widgets with sensible defaults
# These defaults allow %run to work from other notebooks without pre-defining widgets
dbutils.widgets.text("job_date", "2026-07-10", "Job Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("environment", "dev", ["dev", "test", "prod"], "Environment")
dbutils.widgets.text("input_path", "s3://retail-data-lake-pj2026det-test/raw", "Input Path")
dbutils.widgets.text("output_database", "test.test0711", "Output Database (catalog.schema)")
dbutils.widgets.text("batch_id", "", "Batch ID")

print("✓ Job parameters defined")

# COMMAND ----------

# DBTITLE 1,Retrieve & Validate Parameters
# Retrieve parameter values
job_date = dbutils.widgets.get("job_date") or datetime.now().strftime("%Y-%m-%d")
environment = dbutils.widgets.get("environment")
input_path = dbutils.widgets.get("input_path")
output_database = dbutils.widgets.get("output_database")
batch_id = dbutils.widgets.get("batch_id") or str(uuid.uuid4())

# Validate required parameters
errors = []
if not job_date:
    errors.append("job_date is required")
else:
    try:
        datetime.strptime(job_date, "%Y-%m-%d")
    except ValueError:
        errors.append("job_date must be in YYYY-MM-DD format")

if not input_path:
    errors.append("input_path is required")

if not output_database:
    errors.append("output_database is required")
elif "." not in output_database:
    errors.append("output_database must be in format: catalog.schema")

if errors:
    raise ValueError(f"Parameter validation failed:\n" + "\n".join(f"  - {e}" for e in errors))

print("✓ Parameters validated successfully")

# COMMAND ----------

# DBTITLE 1,S3 Handshake Verification
# AWS S3 Handshake Verification (E001 Error Handling)
print("\nVerifying AWS S3 connection...")

try:
    # Attempt to list files in the input path to verify S3 access
    test_path = input_path.rstrip('/') + '/'
    
    # Try to access S3 - this will fail if credentials or path are invalid
    files = dbutils.fs.ls(test_path)
    
    print(f"✓ S3 connection verified: {test_path}")
    print(f"  Accessible directories: {len([f for f in files if f.isDir()])}")
    print(f"  Accessible files: {len([f for f in files if not f.isDir()])}")
    
except Exception as e:
    error_message = f"E001: AWS S3 connection failed for path: {input_path}\nDetails: {str(e)}"
    print(f"\n✗ {error_message}")
    raise ConnectionError(error_message)

# COMMAND ----------

# DBTITLE 1,Display Configuration
# Display job configuration
print("=" * 70)
print("JOB CONFIGURATION")
print("=" * 70)
print(f"Job Date:        {job_date}")
print(f"Environment:     {environment}")
print(f"Input Path:      {input_path}")
print(f"Output Database: {output_database}")
print(f"Batch ID:        {batch_id}")
print(f"Execution Time:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Setup Environment
# Parse output database
catalog_name, schema_name = output_database.split(".")

print("\nInitializing Unity Catalog schemas and tables...")

# Create catalog if not exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
print(f"  ✓ Catalog {catalog_name} ready")

# Create schemas if not exist
schemas_to_create = [
    f"{catalog_name}.silver",
    f"{catalog_name}.audit"
]

for schema in schemas_to_create:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"  ✓ Schema {schema} ready")

# Set current catalog
spark.sql(f"USE CATALOG {catalog_name}")

# Create target tables if not exist
print("\nEnsuring target tables exist...")

# 1. silver.fact_sales
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.fact_sales (
    transaction_id STRING,
    transaction_date DATE,
    transaction_timestamp TIMESTAMP,
    product_id STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    customer_id STRING,
    customer_name STRING,
    customer_segment STRING,
    customer_region STRING,
    store_id STRING,
    store_name STRING,
    store_location STRING,
    store_region STRING,
    quantity INT,
    unit_price DECIMAL(18,2),
    amount DECIMAL(18,2),
    calculated_amount DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    discount_percentage DECIMAL(5,2),
    transaction_year INT,
    transaction_month INT,
    transaction_day INT,
    batch_id STRING,
    job_date STRING,
    processed_timestamp TIMESTAMP,
    source_system STRING
)
USING DELTA
PARTITIONED BY (job_date)
""")
print(f"  ✓ Table {catalog_name}.silver.fact_sales ready")

# 2. silver.reject_sales
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.silver.reject_sales (
    transaction_id STRING,
    raw_data STRING,
    rejection_reason STRING,
    rejection_category STRING,
    batch_id STRING,
    job_date STRING,
    rejected_timestamp TIMESTAMP,
    source_system STRING
)
USING DELTA
PARTITIONED BY (job_date)
""")
print(f"  ✓ Table {catalog_name}.silver.reject_sales ready")

# 3. audit.etl_job_log
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.audit.etl_job_log (
    batch_id STRING,
    job_date STRING,
    job_name STRING,
    environment STRING,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    status STRING,
    records_processed BIGINT,
    records_rejected BIGINT,
    error_message STRING,
    execution_duration_seconds DOUBLE
)
USING DELTA
""")
print(f"  ✓ Table {catalog_name}.audit.etl_job_log ready")

print(f"\n✓ All Unity Catalog schemas and tables initialized")

# COMMAND ----------

# DBTITLE 1,Define Utility Functions
# Utility function: Add audit columns
def add_audit_columns(df, batch_id_val=batch_id, job_date_val=job_date):
    """
    Add standard audit columns to a DataFrame.
    
    Args:
        df: Input DataFrame
        batch_id_val: Batch identifier
        job_date_val: Job date
    
    Returns:
        DataFrame with audit columns added
    """
    return df \
        .withColumn("batch_id", F.lit(batch_id_val)) \
        .withColumn("job_date", F.lit(job_date_val)) \
        .withColumn("processed_timestamp", F.current_timestamp()) \
        .withColumn("source_system", F.lit("sales_transaction_etl"))

# Utility function: Create rejection record
def create_reject_record(transaction_id, raw_data, rejection_reason, rejection_category):
    """
    Create a rejection record for invalid data.
    
    Args:
        transaction_id: Transaction identifier
        raw_data: Raw data as JSON string
        rejection_reason: Detailed rejection reason
        rejection_category: Category of rejection (e.g., NULL_VALUE, INVALID_FORMAT)
    
    Returns:
        DataFrame with rejection record
    """
    reject_data = [{
        "transaction_id": transaction_id,
        "raw_data": raw_data,
        "rejection_reason": rejection_reason,
        "rejection_category": rejection_category,
        "batch_id": batch_id,
        "job_date": job_date,
        "rejected_timestamp": datetime.now(),
        "source_system": "sales_transaction_etl"
    }]
    return spark.createDataFrame(reject_data)

# Utility function: Log job metrics
def log_metrics(step_name, record_count, duration_seconds=None, rejected_count=0):
    """
    Log job execution metrics.
    
    Args:
        step_name: Name of the processing step
        record_count: Number of records processed
        duration_seconds: Execution duration in seconds
        rejected_count: Number of records rejected
    """
    metrics = {
        "batch_id": batch_id,
        "job_date": job_date,
        "step_name": step_name,
        "record_count": record_count,
        "rejected_count": rejected_count,
        "duration_seconds": duration_seconds,
        "timestamp": datetime.now().isoformat()
    }
    print(f"[METRICS] {json.dumps(metrics)}")

print("✓ Utility functions defined")

# COMMAND ----------

# DBTITLE 1,Initialization Summary
# Display initialization summary
print("\n" + "="*70)
print("INITIALIZATION COMPLETE")
print("="*70)
print(f"Catalog:         {catalog_name}")
print(f"Batch ID:        {batch_id}")
print(f"Job Date:        {job_date}")
print(f"Environment:     {environment}")
print(f"Input Path:      {input_path}")
print("\nTarget Tables:")
print(f"  ✓ {catalog_name}.silver.fact_sales")
print(f"  ✓ {catalog_name}.silver.reject_sales")
print(f"  ✓ {catalog_name}.audit.etl_job_log")
print("\nUtility Functions:")
print("  ✓ add_audit_columns()")
print("  ✓ create_reject_record()")
print("  ✓ log_metrics()")
print("="*70)
print("\n✓ Ready to proceed with ETL pipeline")

# COMMAND ----------

# DBTITLE 1,Project Structure Summary
# MAGIC %md
# MAGIC ## Project Structure
# MAGIC
# MAGIC **Workspace Path:** `/Users/mikuni2024@gmail.com/purchase-analysis-platform/job_sales_transaction_etl/`
# MAGIC
# MAGIC ### Notebook Pipeline:
# MAGIC
# MAGIC 1. **00_initialize** - Setup & parameter handling
# MAGIC 2. **01_read_sales** - M001: Load POS transactions
# MAGIC 3. **02_read_master** - M002: Load master tables
# MAGIC 4. **03_validation** - M003: Validation logic
# MAGIC 5. **04_transformation** - M004: Data cleansing & calculations
# MAGIC 6. **05_join_master** - M005: Master table joins
# MAGIC 7. **06_write_silver** - M006: Persistence to Delta Lake
# MAGIC 8. **07_audit_log** - M007: Log auditing
# MAGIC
# MAGIC **Execution Flow:** Run notebooks sequentially using `%run ./notebook_name` or orchestrate via Databricks Job.
