# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M004: Data Transformation
# MAGIC %md
# MAGIC # 23_transformation: Data Cleansing & Calculations (M004)
# MAGIC
# MAGIC This notebook covers:
# MAGIC - Data type conversions
# MAGIC - Data cleansing and string standardization
# MAGIC - Calculation of derived fields
# MAGIC - Application of business logic
# MAGIC - Column renaming and formatting
# MAGIC
# MAGIC **Prerequisite:** Run `22_validation` before this notebook.
# MAGIC
# MAGIC **Output:** `df_sales_transformed` — Cleaned and transformed sales data

# COMMAND ----------

# DBTITLE 1,Run Prerequisites
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Load Validated Data
# MAGIC %run ./20_read_sales

# COMMAND ----------

# DBTITLE 1,Apply Data Cleansing
# Apply data cleansing transformations
# Data cleansing here includes:
# - Converting transaction_time to a proper date (transaction_date)
# - Calculating subtotal (before discount) and net_amount (after discount)
start_time = datetime.now()

print("Applying data transformations...")

df_sales_transformed = df_sales \
    .withColumn("transaction_date", F.to_date(F.col("transaction_time"))) \
    .withColumn("subtotal", F.col("quantity") * F.col("unit_price")) \
    .withColumn("net_amount", F.col("subtotal") - F.col("discount_amount"))

print("✓ Data types converted")

# COMMAND ----------

# DBTITLE 1,Calculate Derived Fields
# Calculate derived fields
# Note: discount_amount already exists from source data, subtotal calculated in previous step
df_sales_transformed = df_sales_transformed \
    .withColumn("transaction_year", F.year(F.col("transaction_date"))) \
    .withColumn("transaction_month", F.month(F.col("transaction_date"))) \
    .withColumn("transaction_day", F.dayofmonth(F.col("transaction_date"))) \
    .withColumn("transaction_weekday", F.dayofweek(F.col("transaction_date")))

print("✓ Derived fields calculated")

# COMMAND ----------

# DBTITLE 1,Standardize String Fields
# Standardize string fields (exclude ID columns to preserve foreign key integrity)
id_columns = {"transaction_id", "customer_id", "product_id", "store_id"}
string_columns = [field.name for field in df_sales_transformed.schema.fields 
                  if str(field.dataType) == "StringType" and field.name not in id_columns]

# Use withColumns for better performance (avoids nested execution plan)
if string_columns:
    standardized_cols = {col_name: F.trim(F.upper(F.col(col_name))) for col_name in string_columns}
    df_sales_transformed = df_sales_transformed.withColumns(standardized_cols)
    print(f"✓ Standardized {len(string_columns)} string columns (excluding ID columns)")
else:
    print("✓ No non-ID string columns to standardize")

# COMMAND ----------

# DBTITLE 1,Display Transformation Results
# Display transformation summary
duration = (datetime.now() - start_time).total_seconds()
transformed_count = df_sales_transformed.count()

log_metrics("transform_sales", transformed_count, duration)

print("\n" + "="*60)
print("TRANSFORMATION SUMMARY")
print("="*60)
print(f"Records Processed: {transformed_count:,}")
print(f"Duration: {duration:.2f} seconds")
print("="*60)

print("\nTransformed Data Sample:")
display(df_sales_transformed.limit(10))
