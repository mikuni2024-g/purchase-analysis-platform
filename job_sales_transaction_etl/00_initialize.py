# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC # 00_initialize: Setup & Parameter Handling

# COMMAND ----------

# DBTITLE 1,Project Structure
# MAGIC %md
# MAGIC ## Project Structure
# MAGIC
# MAGIC **Workspace Path:** `/Users/mikuni2024@gmail.com/purchase-analysis-platform/job_sales_transaction_etl/`
# MAGIC
# MAGIC **Execution Flow:**
# MAGIC * **Step 1 - Initialize**: `%run ./00_initialize`
# MAGIC * **Step 2 - Bronze Ingestion** (parallel execution recommended)
# MAGIC * **Step 3 - Silver Processing** (sequential execution):
# MAGIC   - `%run ./20_read_sales` → ... → `%run ./26_audit_log`
# MAGIC * **Orchestration**: Use Databricks Job/Workflow for production scheduling

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
# dbutils.widgets.text("job_date", "2026-07-10", "Job Date (YYYY-MM-DD)")
# dbutils.widgets.dropdown("environment", "dev", ["dev", "test", "prod"], "Environment")
# dbutils.widgets.text("input_path", "s3://retail-data-lake-pj2026det-test/raw", "Input Path")
# dbutils.widgets.text("output_database", "test.test0711", "Output Database (catalog.schema)")
# dbutils.widgets.text("batch_id", "", "Batch ID")

# print("✓ Job parameters defined")

# COMMAND ----------

# DBTITLE 1,Retrieve & Validate Parameters



# Retrieve parameter values
# job_date = dbutils.widgets.get("job_date") or datetime.now().strftime("%Y-%m-%d")
# environment = dbutils.widgets.get("environment")
# input_path = dbutils.widgets.get("input_path")
# output_database = dbutils.widgets.get("output_database")
# batch_id = dbutils.widgets.get("batch_id") or str(uuid.uuid4())

job_date = "2026-07-10"
environment = "dev"
input_path = "s3://retail-data-lake-pj2026det-test/raw"
output_database = "test.test0711"
batch_id = str(uuid.uuid4())

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

print("\nInitializing Unity Catalog schemas...")

# Create catalog if not exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
print(f"  ✓ Catalog {catalog_name} ready")

# Create schemas if not exist
schemas_to_create = [
    f"{catalog_name}.bronze",
    f"{catalog_name}.silver",
    f"{catalog_name}.audit"
]

for schema in schemas_to_create:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"  ✓ Schema {schema} ready")

# Set current catalog
spark.sql(f"USE CATALOG {catalog_name}")

print("\n✓ Catalog and schemas initialized")


# COMMAND ----------

# DBTITLE 1,Create Bronze Layer Tables
# BRONZE LAYER TABLES - Raw Data Landing Zone (13 tables)
print("\nCreating Bronze Layer tables...")
print("\n[Bronze Layer - Transactional Data]")

# 1. bronze.pos_transactions (POS sales)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.pos_transactions (
    transaction_id STRING,
    customer_id STRING,
    product_id STRING,
    store_id STRING,
    quantity STRING,
    unit_price STRING,
    discount_amount STRING,
    transaction_time STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING,
    _job_date STRING
)
USING DELTA
PARTITIONED BY (_job_date)
""")
print(f"  ✓ Table {catalog_name}.bronze.pos_transactions ready")

# 2. bronze.online_orders (E-commerce)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.online_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity STRING,
    price STRING,
    discount STRING,
    order_timestamp STRING,
    payment_method STRING,
    shipping_address STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING,
    _job_date STRING
)
USING DELTA
PARTITIONED BY (_job_date)
""")
print(f"  ✓ Table {catalog_name}.bronze.online_orders ready")

# 3. bronze.web_clickstream (Web analytics)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.web_clickstream (
    session_id STRING,
    user_id STRING,
    event_type STRING,
    page_url STRING,
    referrer STRING,
    event_timestamp STRING,
    user_agent STRING,
    ip_address STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING,
    _job_date STRING
)
USING DELTA
PARTITIONED BY (_job_date)
""")
print(f"  ✓ Table {catalog_name}.bronze.web_clickstream ready")

print("\n[Bronze Layer - CRM Master Data]")

# 4. bronze.customer_master
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.customer_master (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    phone STRING,
    birth_date STRING,
    gender STRING,
    registration_date STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
""")
print(f"  ✓ Table {catalog_name}.bronze.customer_master ready")

# 5. bronze.loyalty_members
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.loyalty_members (
    customer_id STRING,
    membership_tier STRING,
    points_balance STRING,
    join_date STRING,
    expiry_date STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
""")
print(f"  ✓ Table {catalog_name}.bronze.loyalty_members ready")

# 6. bronze.coupon_dispatches
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.coupon_dispatches (
    coupon_id STRING,
    customer_id STRING,
    coupon_code STRING,
    discount_type STRING,
    discount_value STRING,
    dispatch_date STRING,
    expiry_date STRING,
    status STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
""")
print(f"  ✓ Table {catalog_name}.bronze.coupon_dispatches ready")

print("\n[Bronze Layer - ERP Master Data]")

# 7. bronze.product_master
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.product_master (
    product_id STRING,
    product_name STRING,
    category_id STRING,
    unit_price STRING,
    cost_price STRING,
    supplier_id STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
""")
print(f"  ✓ Table {catalog_name}.bronze.product_master ready")

# 8. bronze.category_master
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.category_master (
    category_id STRING,
    category_name STRING,
    parent_category STRING,
    category_level STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
""")
print(f"  ✓ Table {catalog_name}.bronze.category_master ready")

# 9. bronze.supplier_master
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.supplier_master (
    supplier_id STRING,
    supplier_name STRING,
    contact_person STRING,
    phone STRING,
    email STRING,
    address STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
""")
print(f"  ✓ Table {catalog_name}.bronze.supplier_master ready")

# 10. bronze.store_master
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.store_master (
    store_id STRING,
    store_name STRING,
    region STRING,
    open_date STRING,
    store_type STRING,
    manager_name STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
""")
print(f"  ✓ Table {catalog_name}.bronze.store_master ready")

# 11. bronze.inventory_balance
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.inventory_balance (
    product_id STRING,
    store_id STRING,
    quantity_on_hand STRING,
    quantity_reserved STRING,
    last_updated STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
""")
print(f"  ✓ Table {catalog_name}.bronze.inventory_balance ready")

print("\n[Bronze Layer - Reference Data]")

# 12. bronze.retail_calendar
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.retail_calendar (
    date STRING,
    year STRING,
    month STRING,
    day STRING,
    day_of_week STRING,
    fiscal_year STRING,
    fiscal_quarter STRING,
    is_retail_peak STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
""")
print(f"  ✓ Table {catalog_name}.bronze.retail_calendar ready")

# 13. bronze.holiday_calendar
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.holiday_calendar (
    date STRING,
    holiday_name STRING,
    holiday_type STRING,
    country STRING,
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
""")
print(f"  ✓ Table {catalog_name}.bronze.holiday_calendar ready")

print(f"\n✓ Bronze Layer: 13 tables created")

# COMMAND ----------

# DBTITLE 1,Create Silver Layer Tables
# SILVER LAYER TABLES - Validated & Enriched Data
print("\nCreating Silver Layer tables...")

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

print(f"\n✓ Silver Layer: 2 tables created")

# COMMAND ----------

# DBTITLE 1,Create Audit Tables
# AUDIT TABLES - Job Execution Logging
print("\nCreating Audit tables...")

# 1. audit.etl_job_log
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

print(f"\n✓ Audit Layer: 1 table created")
print(f"\n{'='*70}")
print(f"\u2713 ALL TABLES CREATED: 13 Bronze + 2 Silver + 1 Audit = 16 tables")
print(f"{'='*70}")

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
print("\n" + "="*70)
print("LAYER SUMMARY")
print("="*70)
print("\n✓ Bronze Layer (13 tables):")
print(f"  Transactional: pos_transactions, online_orders, web_clickstream")
print(f"  CRM: customer_master, loyalty_members, coupon_dispatches")
print(f"  ERP: product_master, category_master, supplier_master, store_master, inventory_balance")
print(f"  Reference: retail_calendar, holiday_calendar")
print("\n✓ Silver Layer (2 tables):")
print(f"  fact_sales, reject_sales")
print("\n✓ Audit Layer (1 table):")
print(f"  etl_job_log")
print("\n" + "="*70)
print("UTILITY FUNCTIONS")
print("="*70)
print("  ✓ add_audit_columns()")
print("  ✓ create_reject_record()")
print("  ✓ log_metrics()")
print("="*70)
print("\n✓ Ready to proceed with ETL pipeline")
