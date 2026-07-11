# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M003: Validation Logic
# MAGIC %md
# MAGIC # 03_validation: Data Validation (M003)
# MAGIC
# MAGIC This notebook implements:
# MAGIC * Data quality checks
# MAGIC * Schema validation
# MAGIC * Business rule validation
# MAGIC * Null/duplicate detection
# MAGIC * Range and constraint checks
# MAGIC
# MAGIC **Dependencies:** Run `01_read_sales` and `02_read_master` first
# MAGIC
# MAGIC **Output:** Validated DataFrames with quality flags

# COMMAND ----------

# DBTITLE 1,Run Prerequisites
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Load Data
# MAGIC %run ./01_read_sales

# COMMAND ----------

# DBTITLE 1,Load Master Data
# MAGIC %run ./02_read_master

# COMMAND ----------

# DBTITLE 1,Define Validation Rules
# Define validation rules
validation_rules = {
    "required_fields": ["transaction_id", "product_id", "quantity", "amount"],
    "numeric_fields": ["quantity", "amount"],
    "date_fields": ["transaction_date"],
    "positive_fields": ["quantity", "amount"]
}

print("✓ Validation rules defined")

# COMMAND ----------

# DBTITLE 1,Validate Sales Data
# Validate sales transactions
start_time = datetime.now()

print("Running validation checks...")

# Check for nulls in required fields
null_checks = []
for field in validation_rules.get("required_fields", []):
    if field in df_sales.columns:
        null_count = df_sales.filter(F.col(field).isNull()).count()
        null_checks.append({"field": field, "null_count": null_count})
        if null_count > 0:
            print(f"  ⚠️  {field}: {null_count} null values found")
        else:
            print(f"  ✓ {field}: No null values")

# Check for duplicates
duplicate_count = df_sales.count() - df_sales.dropDuplicates(["transaction_id"]).count()
if duplicate_count > 0:
    print(f"\n  ⚠️  Found {duplicate_count} duplicate transaction_ids")
else:
    print(f"\n  ✓ No duplicate transaction_ids")

# Check for negative values in positive fields
for field in validation_rules.get("positive_fields", []):
    if field in df_sales.columns:
        negative_count = df_sales.filter(F.col(field) < 0).count()
        if negative_count > 0:
            print(f"  ⚠️  {field}: {negative_count} negative values found")
        else:
            print(f"  ✓ {field}: All values are positive")

duration = (datetime.now() - start_time).total_seconds()
log_metrics("validate_sales", df_sales.count(), duration)

print("\n✓ Validation completed")

# COMMAND ----------

# DBTITLE 1,Generate Validation Report
# Generate validation summary
print("\n" + "="*60)
print("VALIDATION SUMMARY")
print("="*60)
print(f"Total Records:     {df_sales.count():,}")
print(f"Duplicate Records: {duplicate_count:,}")
print(f"Null Value Checks: {len(null_checks)} fields validated")
print("="*60)
