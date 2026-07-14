# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M003: Validation Logic
# MAGIC %md
# MAGIC # 22_validation: Data Validation (M003)
# MAGIC
# MAGIC This notebook implements:
# MAGIC * Data quality checks
# MAGIC * Schema validation
# MAGIC * Business rule validation
# MAGIC * Null/duplicate detection
# MAGIC * Range and constraint checks
# MAGIC
# MAGIC **Dependencies:** Run `20_read_sales` and `21_read_master` first
# MAGIC
# MAGIC **Output:** Validated DataFrames with quality flags

# COMMAND ----------

# DBTITLE 1,Run Prerequisites
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Load Data
# MAGIC %run ./20_read_sales

# COMMAND ----------

# DBTITLE 1,Load Master Data
# MAGIC %run ./21_read_master

# COMMAND ----------

# DBTITLE 1,Define Validation Rules
# Define validation rules (matched to actual df_sales schema)
validation_rules = {
    "required_fields": ["transaction_id", "product_id", "store_id", "quantity", "unit_price"],
    "numeric_fields": ["quantity", "unit_price", "discount_amount"],
    "positive_fields": ["quantity", "unit_price"],
    "foreign_keys": {
        "product_id": "df_product_master",
        "customer_id": "df_customer_master",
        "store_id": "df_store_master"
    }
}

print("✓ Validation rules defined")

# COMMAND ----------

# DBTITLE 1,Validate Sales Data
# Validate sales transactions
start_time = datetime.now()

print("Running validation checks...")

# Check for nulls in required fields
null_checks = []
# Iterate through required fields for null checks
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

# DBTITLE 1,Validate Referential Integrity
# Validate referential integrity (foreign keys)
print("\nValidating referential integrity...")

referential_checks = []

# Check product_id exists in product_master
invalid_products = df_sales \
    .join(df_product_master, "product_id", "left_anti") \
    .select("product_id").distinct() \
    .count()

if invalid_products > 0:
    print(f"  ⚠️  product_id: {invalid_products} invalid references")
    referential_checks.append({"field": "product_id", "invalid_count": invalid_products})
else:
    print(f"  ✓ product_id: All references valid")

# Check customer_id exists in customer_master (skip nulls for guest purchases)
invalid_customers = df_sales \
    .filter(F.col("customer_id").isNotNull()) \
    .join(df_customer_master, "customer_id", "left_anti") \
    .select("customer_id").distinct() \
    .count()

if invalid_customers > 0:
    print(f"  ⚠️  customer_id: {invalid_customers} invalid references")
    referential_checks.append({"field": "customer_id", "invalid_count": invalid_customers})
else:
    print(f"  ✓ customer_id: All references valid (nulls allowed for guest purchases)")

# Check store_id exists in store_master
invalid_stores = df_sales \
    .join(df_store_master, "store_id", "left_anti") \
    .select("store_id").distinct() \
    .count()

if invalid_stores > 0:
    print(f"  ⚠️  store_id: {invalid_stores} invalid references")
    referential_checks.append({"field": "store_id", "invalid_count": invalid_stores})
else:
    print(f"  ✓ store_id: All references valid")

# Validate product categories exist
invalid_categories = df_product_master \
    .join(df_category_master, "category_id", "left_anti") \
    .select("category_id").distinct() \
    .count()

if invalid_categories > 0:
    print(f"  ⚠️  category_id (product): {invalid_categories} invalid references")
    referential_checks.append({"field": "category_id", "invalid_count": invalid_categories})
else:
    print(f"  ✓ category_id: All product categories valid")

print("\n✓ Referential integrity validation completed")

# COMMAND ----------

# DBTITLE 1,Generate Validation Report
# Generate validation summary
print("\n" + "="*60)
print("VALIDATION SUMMARY")
print("="*60)
print(f"Total Records:           {df_sales.count():,}")
print(f"Duplicate Records:       {duplicate_count:,}")
print(f"Null Value Checks:       {len(null_checks)} fields validated")
print(f"Referential Integrity:   {len(referential_checks)} issues found" if referential_checks else "Referential Integrity:   All valid")
print("="*60)

# Show details if issues found
if null_checks:
    null_issues = [check for check in null_checks if check["null_count"] > 0]
    if null_issues:
        print("\n⚠️  Null Value Issues:")
        for issue in null_issues:
            print(f"  - {issue['field']}: {issue['null_count']} nulls")

if referential_checks:
    print("\n⚠️  Referential Integrity Issues:")
    for issue in referential_checks:
        print(f"  - {issue['field']}: {issue['invalid_count']} invalid references")
