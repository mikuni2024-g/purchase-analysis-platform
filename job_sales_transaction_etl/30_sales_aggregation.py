# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Sales Aggregation Overview
# MAGIC %md
# MAGIC # 30_sales_aggregation: 
# MAGIC
# MAGIC This notebook creates business metrics and summaries from Silver layer data.
# MAGIC
# MAGIC **Gold Layer - Business Metrics:**
# MAGIC * Daily sales summary (日次売上集計)
# MAGIC * Customer analytics (顧客分析)
# MAGIC * Product performance (商品分析)
# MAGIC * Time-based trends (時系列トレンド)
# MAGIC
# MAGIC **Dependencies:** Silver layer tables must exist
# MAGIC **Output:** Gold layer aggregated tables and metrics

# COMMAND ----------

# DBTITLE 1,Run Initialization
# MAGIC %run /Users/mikuni2024@gmail.com/purchase-analysis-platform/job_sales_transaction_etl/00_initialize

# COMMAND ----------

# DBTITLE 1,Setup Gold Layer Schema
# Create Gold layer schema for business metrics
gold_schema = f"{catalog_name}.gold"

print("Creating Gold layer schema...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")
print(f"✓ Schema {gold_schema} ready\n")

# Define source table
source_table = f"{catalog_name}.silver.fact_sales"

print(f"Source: {source_table}")
print(f"Target: {gold_schema}.*")

display(spark.table(source_table).limit(5))

# COMMAND ----------

# DBTITLE 1,Daily Sales Summary (日次売上集計)
# 1. Daily Sales Summary - Business-level metrics by day
print("Creating daily sales summary...\n")

df_daily_sales = spark.table(source_table).groupBy(
    "transaction_date"
).agg(
    # Transaction metrics
    F.countDistinct("transaction_id").alias("transaction_count"),
    F.sum("quantity").alias("total_quantity"),
    
    # Revenue metrics
    F.sum("amount").alias("total_sales"),
    F.sum("discount_amount").alias("total_discount"),
    
    # Average metrics
    # amount = quantity x unit_price
    F.avg("amount").alias("avg_transaction_value"),
    F.avg("quantity").alias("avg_items_per_transaction"),
    F.avg("discount_percentage").alias("avg_discount_rate"),
    
    # Customer metrics
    F.countDistinct("customer_id").alias("unique_customers"),
    
    # Product metrics
    F.countDistinct("product_id").alias("unique_products_sold")
).orderBy("transaction_date")

print("Sample daily sales summary:")
display(df_daily_sales.limit(5))

print(f"\nTotal days: {df_daily_sales.count()}")


# what kind of charts can we make with this data?
# - Daily Sales Summary - Business-level metrics by day

# For Daily Sales Summary (日次売上集計) data, the best visualizations are:

# 📊 Recommended Charts
# 1. Line Chart ⭐ Best for Time Trends
# Perfect for tracking sales performance over time:

# Total Sales trend line
# Transaction Count trend
# Unique Customers trend
# When to use: Multiple days of data, showing growth/decline patterns

# 2. Combo Chart ⭐ Multiple Metrics Together
# Combines bars + lines for different scales:

# Bars: Total Sales (¥ amount)
# Line: Transaction Count (number)
# When to use: Comparing revenue vs volume metrics

# 3. Area Chart - Volume Over Time
# Shows cumulative magnitude:

# Stacked: Total Sales, Discounts, Net Sales
# Single: Revenue trend with filled area
# When to use: Emphasizing total volume/magnitude

# 4. Bar Chart - Daily Comparisons
# Side-by-side daily totals:

# Compare weekday vs weekend sales
# Monthly day-by-day comparison
# When to use: Discrete daily comparisons

# 🎯 Best Choice for Your Data
# Since you currently have 1 day of data, I recommend:

# For Now: Summary Cards + Single Bar
# Display key metrics as cards until you have more days

# For Future (with more days):
# Combo Chart showing:

# Primary Y-axis (bars): Total Sales (¥)
# Secondary Y-axis (line): Transaction Count
# This lets you see both revenue and customer activity trends together!





# COMMAND ----------

# DBTITLE 1,Customer Analytics (顧客分析)
# 2. Customer Analytics - Customer-level behavior and value
print("Creating customer analytics...\n")

df_customer_metrics = spark.table(source_table).groupBy(
    "customer_id"
).agg(
    # Purchase frequency
    F.countDistinct("transaction_id").alias("total_transactions"),
    F.countDistinct("transaction_date").alias("purchase_days"),
    
    # Revenue contribution
    F.sum("amount").alias("lifetime_value"),
    F.avg("amount").alias("avg_transaction_value"),
    
    # Product diversity
    F.sum("quantity").alias("total_items_purchased"),
    F.countDistinct("product_id").alias("unique_products_purchased"),
    
    # Recency
    F.max("transaction_date").alias("last_purchase_date"),
    F.min("transaction_date").alias("first_purchase_date")
).withColumn(
    "customer_tenure_days",
    F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date"))
).withColumn(
    "purchase_frequency",
    F.when(F.col("customer_tenure_days") > 0,
           F.col("total_transactions") / F.col("customer_tenure_days"))
    .otherwise(F.col("total_transactions"))
).orderBy(F.col("lifetime_value").desc())

print("Top 10 customers by lifetime value:")
df_customer_metrics.show(10, truncate=False)
display(df_customer_metrics)

print(f"\nTotal customers: {df_customer_metrics.count()}")

# COMMAND ----------

# DBTITLE 1,Product Performance (商品分析)
# 3. Product Performance - Product-level sales metrics
print("Creating product performance metrics...\n")

df_product_metrics = spark.table(source_table).groupBy(
    "product_id",
    "product_name",
    "category_id"
).agg(
    # Sales volume
    F.sum("quantity").alias("total_units_sold"),
    F.countDistinct("transaction_id").alias("times_purchased"),
    
    # Revenue metrics
    F.sum("amount").alias("total_revenue"),
    F.avg("unit_price").alias("avg_unit_price"),
    
    # Customer reach
    F.countDistinct("customer_id").alias("unique_customers"),
    
    # Date range
    F.min("transaction_date").alias("first_sale_date"),
    F.max("transaction_date").alias("last_sale_date")
).withColumn(
    "avg_revenue_per_transaction",
    F.col("total_revenue") / F.col("times_purchased")
).orderBy(F.col("total_revenue").desc())

print("Top 10 products by revenue:")
df_product_metrics.show(10, truncate=False)

print(f"\nTotal products: {df_product_metrics.count()}")

# COMMAND ----------

# DBTITLE 1,Category Performance (カテゴリ分析)
# 4. Category Performance - Category-level insights
print("Creating category performance metrics...\n")

df_category_metrics = spark.table(source_table).groupBy(
    "category_id"
).agg(
    # Sales volume
    F.sum("quantity").alias("total_units_sold"),
    F.countDistinct("product_id").alias("products_in_category"),
    F.countDistinct("transaction_id").alias("transactions"),
    
    # Revenue
    F.sum("amount").alias("total_revenue"),
    F.avg("amount").alias("avg_transaction_value"),
    
    # Customer engagement
    F.countDistinct("customer_id").alias("unique_customers")
).withColumn(
    "revenue_per_product",
    F.col("total_revenue") / F.col("products_in_category")
).orderBy(F.col("total_revenue").desc())

print("Category performance:")
df_category_metrics.show(truncate=False)

# Calculate category revenue share
total_revenue = df_category_metrics.agg(F.sum("total_revenue")).collect()[0][0]

df_category_share = df_category_metrics.withColumn(
    "revenue_share_pct",
    (F.col("total_revenue") / total_revenue * 100).cast("decimal(5,2)")
).orderBy(F.col("revenue_share_pct").desc())

print("\nCategory revenue share:")
df_category_share.select("category_id", "total_revenue", "revenue_share_pct").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Write to Gold Layer
# Write aggregated tables to Gold layer
print("\n" + "="*70)
print("WRITING TO GOLD LAYER")
print("="*70)

# 1. Daily sales summary
table_daily = f"{gold_schema}.daily_sales_summary"
print(f"\nWriting: {table_daily}")
df_daily_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_daily)
print(f"✓ Written {df_daily_sales.count()} records")

# 2. Customer metrics
table_customer = f"{gold_schema}.customer_metrics"
print(f"\nWriting: {table_customer}")
df_customer_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_customer)
print(f"✓ Written {df_customer_metrics.count()} records")

# 3. Product metrics
table_product = f"{gold_schema}.product_metrics"
print(f"\nWriting: {table_product}")
df_product_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_product)
print(f"✓ Written {df_product_metrics.count()} records")

# 4. Category metrics
table_category = f"{gold_schema}.category_metrics"
print(f"\nWriting: {table_category}")
df_category_share.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_category)
print(f"✓ Written {df_category_share.count()} records")

print("\n" + "="*70)
print("✓ ALL GOLD LAYER TABLES CREATED")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Display Summary
# Display final summary
print("\n" + "="*70)
print("SALES AGGREGATION SUMMARY (売上集計サマリー)")
print("="*70)

print(f"\n📊 Gold Layer Tables Created:")
print(f"   1. {table_daily}")
print(f"   2. {table_customer}")
print(f"   3. {table_product}")
print(f"   4. {table_category}")

print(f"\n📈 Key Metrics:")
print(f"   • Total Days Analyzed: {df_daily_sales.count()}")
print(f"   • Total Customers: {df_customer_metrics.count()}")
print(f"   • Total Products: {df_product_metrics.count()}")
print(f"   • Total Categories: {df_category_share.count()}")

print(f"\n💰 Overall Performance:")
total_metrics = spark.table(source_table).agg(
    F.sum("amount").alias("total_revenue"),
    F.countDistinct("transaction_id").alias("total_transactions"),
    F.sum("quantity").alias("total_items_sold")
).collect()[0]

print(f"   • Total Revenue: ¥{total_metrics['total_revenue']:,.0f}")
print(f"   • Total Transactions: {total_metrics['total_transactions']:,}")
print(f"   • Total Items Sold: {total_metrics['total_items_sold']:,}")

print("\n" + "="*70)
print("✓ Sales aggregation complete!")
print("="*70)
