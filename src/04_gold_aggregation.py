# Databricks notebook source
# MAGIC %md
# MAGIC # Task 4 — Gold Aggregation
# MAGIC Reads Silver cleaned transactions → produces two Gold KPI tables:
# MAGIC - `gold_customer_segment_kpis` — Revenue KPIs by segment × category
# MAGIC - `gold_daily_revenue`          — Daily revenue trend by segment

# COMMAND ----------

dbutils.widgets.text("catalog", "agent_monitoring")
dbutils.widgets.text("schema",  "default")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

print(f"catalog={catalog}  schema={schema}")

# COMMAND ----------

from pyspark.sql import functions as F

source_table = f"{catalog}.{schema}.silver_cleaned_transactions"
kpi_table    = f"{catalog}.{schema}.gold_customer_segment_kpis"
daily_table  = f"{catalog}.{schema}.gold_daily_revenue"

print(f"🚀 Gold aggregation starting")
print(f"   Source : {source_table}")

# COMMAND ----------

df_silver    = spark.table(source_table)
silver_count = df_silver.count()
print(f"   Rows read from Silver: {silver_count:,}")

# COMMAND ----------

# ── Aggregate 1: Customer Segment × Category KPIs ────────────────────────────
df_kpis = (
    df_silver
    .groupBy("customer_segment", "category")
    .agg(
        F.round(F.sum("net_amount"),      2).alias("total_revenue"),
        F.count("transaction_id")          .alias("transaction_count"),
        F.countDistinct("customer_id")     .alias("unique_customers"),
        F.round(F.avg("net_amount"),      2).alias("avg_order_value"),
        F.round(F.avg("discount_pct"),    2).alias("avg_discount_pct"),
        F.round(F.avg("quantity"),        2).alias("avg_quantity"),
        F.round(F.sum(F.when(F.col("status") == "Completed", F.col("net_amount")).otherwise(0)), 2)
         .alias("completed_revenue"),
        F.round(F.sum(F.when(F.col("status") == "Refunded", F.col("net_amount")).otherwise(0)), 2)
         .alias("refunded_revenue"),
    )
    .withColumn("revenue_per_customer",
        F.round(F.col("total_revenue") / F.col("unique_customers"), 2))
    .withColumn("_aggregated_at", F.current_timestamp())
    .withColumn("_layer",         F.lit("gold"))
    .orderBy(F.col("total_revenue").desc())
)

(df_kpis.write.format("delta")
 .mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(kpi_table))

segment_rows = spark.table(kpi_table).count()
print(f"   ✅ Segment KPIs : {segment_rows:,} rows → '{kpi_table}'")

# COMMAND ----------

# ── Aggregate 2: Daily Revenue Trend ─────────────────────────────────────────
df_daily = (
    df_silver
    .withColumn("txn_day", F.to_date(F.col("transaction_date")))
    .groupBy("txn_day", "customer_segment")
    .agg(
        F.round(F.sum("net_amount"),      2).alias("daily_revenue"),
        F.count("transaction_id")          .alias("daily_transaction_count"),
        F.countDistinct("customer_id")     .alias("daily_unique_customers"),
        F.round(F.avg("net_amount"),      2).alias("daily_avg_order_value"),
    )
    .withColumn("_aggregated_at", F.current_timestamp())
    .withColumn("_layer",         F.lit("gold"))
    .orderBy("txn_day", "customer_segment")
)

(df_daily.write.format("delta")
 .mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(daily_table))

daily_rows = spark.table(daily_table).count()
print(f"   ✅ Daily revenue : {daily_rows:,} rows → '{daily_table}'")

# COMMAND ----------

print(
    f"✅ Gold aggregation complete — "
    f"{segment_rows:,} segment KPI rows | {daily_rows:,} daily revenue rows"
)
