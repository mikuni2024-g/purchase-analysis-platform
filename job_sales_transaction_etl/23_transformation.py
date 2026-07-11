# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M004: Data Transformation
# MAGIC %md
# MAGIC # 04_transformation: Data Cleansing & Calculations (M004)
# MAGIC
# MAGIC This notebook performs:
# MAGIC * Data type conversions
# MAGIC * Data cleansing and standardization
# MAGIC * Derived field calculations
# MAGIC * Business logic transformations
# MAGIC * Column renaming and formatting
# MAGIC
# MAGIC **Dependencies:** Run `03_validation` first
# MAGIC
# MAGIC **Output:** `df_sales_transformed` - Cleaned and transformed sales data

# COMMAND ----------

# DBTITLE 1,Run Prerequisites
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Load Validated Data
# MAGIC %run ./01_read_sales

# COMMAND ----------

# DBTITLE 1,Apply Data Cleansing
# Apply data cleansing transformations
start_time = datetime.now()

print("Applying data transformations...")

df_sales_transformed = df_sales \
    .withColumn("transaction_date", F.to_date(F.col("transaction_time"))) \
    .withColumn("transaction_timestamp", F.col("transaction_time")) \
    .withColumn("amount", (F.col("quantity") * F.col("unit_price")) - F.col("discount_amount"))

print("✓ Data types converted")

# COMMAND ----------

# DBTITLE 1,Calculate Derived Fields
# Calculate derived fields
# Note: discount_amount already exists from source data
df_sales_transformed = df_sales_transformed \
    .withColumn("calculated_amount", F.col("quantity") * F.col("unit_price")) \
    .withColumn("discount_percentage", 
                F.when(F.col("calculated_amount") > 0,
                       (F.col("discount_amount") / F.col("calculated_amount")) * 100)
                .otherwise(0)) \
    .withColumn("transaction_year", F.year(F.col("transaction_date"))) \
    .withColumn("transaction_month", F.month(F.col("transaction_date"))) \
    .withColumn("transaction_day", F.dayofmonth(F.col("transaction_date"))) \
    .withColumn("transaction_weekday", F.dayofweek(F.col("transaction_date")))

print("✓ Derived fields calculated")

# COMMAND ----------

# DBTITLE 1,Standardize String Fields
# Standardize string fields
string_columns = [field.name for field in df_sales_transformed.schema.fields 
                  if str(field.dataType) == "StringType"]

for col_name in string_columns:
    df_sales_transformed = df_sales_transformed \
        .withColumn(col_name, F.trim(F.upper(F.col(col_name))))

print(f"✓ Standardized {len(string_columns)} string columns")

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
