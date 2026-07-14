# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,M005: Master Table Joins
# MAGIC %md
# MAGIC # 24_join_master: Master Table Joins (M005)
# MAGIC
# MAGIC This notebook performs:
# MAGIC * Joining sales transactions with master tables
# MAGIC * Product dimension enrichment
# MAGIC * Customer dimension enrichment
# MAGIC * Store dimension enrichment
# MAGIC * Creating the final enriched dataset
# MAGIC
# MAGIC **Dependencies:** Run `23_transformation` and `21_read_master` first
# MAGIC
# MAGIC **Output:** `df_sales_enriched` - Fully enriched sales data

# COMMAND ----------

# DBTITLE 1,Run Prerequisites
# MAGIC %run ./00_initialize

# COMMAND ----------

# DBTITLE 1,Load Transformed Data
# MAGIC %run ./23_transformation

# COMMAND ----------

# DBTITLE 1,Load Master Tables
# MAGIC %run ./21_read_master

# COMMAND ----------

# DBTITLE 1,Join Product Master
# Join with product master
start_time = datetime.now()

print("Joining with product master...")

df_with_product = df_sales_transformed.alias("sales").join(
    df_product_master.alias("product"),
    F.col("sales.product_id") == F.col("product.product_id"),
    "left"
).select(
    F.col("sales.*"),
    F.col("product.product_name"),
    F.col("product.category_id")
)

print(f"✓ Product master joined: {df_with_product.count():,} records")

# COMMAND ----------

# DBTITLE 1,Join Customer Master
# Join with customer master
print("Joining with customer master...")

df_with_customer = df_with_product.alias("sales").join(
    df_customer_master.alias("customer"),
    F.col("sales.customer_id") == F.col("customer.customer_id"),
    "left"
).select(
    F.col("sales.*"),
    F.col("customer.customer_name")
)

print(f"✓ Customer master joined: {df_with_customer.count():,} records")

# COMMAND ----------

# DBTITLE 1,Join Store Master
# Join with store master
print("Joining with store master...")

df_sales_enriched = df_with_customer.alias("sales").join(
    df_store_master.alias("store"),
    F.col("sales.store_id") == F.col("store.store_id"),
    "left"
).select(
    F.col("sales.*"),
    F.col("store.store_name"),
    F.col("store.region").alias("store_region")
)

enriched_count = df_sales_enriched.count()
duration = (datetime.now() - start_time).total_seconds()

log_metrics("join_master_tables", enriched_count, duration)

print(f"✓ Store master joined: {enriched_count:,} records")
print(f"\n✓ All master joins completed in {duration:.2f} seconds")

# COMMAND ----------

# DBTITLE 1,Display Enriched Data
# Display enriched data sample
print("\n" + "="*60)
print("ENRICHED DATA SUMMARY")
print("="*60)
print(f"Total Records: {df_sales_enriched.count():,}")
print(f"Total Columns: {len(df_sales_enriched.columns)}")
print("="*60)

print("\nEnriched Data Sample:")
display(df_sales_enriched.limit(10))

print("\nEnriched Data Schema:")
df_sales_enriched.printSchema()
