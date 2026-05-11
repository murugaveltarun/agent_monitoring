# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3 — Silver Transform
# MAGIC Cleans, validates and deduplicates Bronze data → Silver layer.
# MAGIC
# MAGIC **Failure simulation:** set `simulate_failure=true` as a job parameter to
# MAGIC trigger a deliberate `ValueError` after Bronze is read — useful for testing
# MAGIC the monitoring agent.

# COMMAND ----------

dbutils.widgets.text("catalog",          "agent_monitoring")
dbutils.widgets.text("schema",           "default")
dbutils.widgets.text("simulate_failure", "false")

catalog  = dbutils.widgets.get("catalog")
schema   = dbutils.widgets.get("schema")
simulate = dbutils.widgets.get("simulate_failure").strip().lower() == "true"

print(f"catalog={catalog}  schema={schema}  simulate_failure={simulate}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

source_table    = f"{catalog}.{schema}.bronze_raw_transactions"
target_table    = f"{catalog}.{schema}.silver_cleaned_transactions"
quarantine_table= f"{catalog}.{schema}.silver_rejected_transactions"

print(f"🚀 Silver transform starting")
print(f"   Source     : {source_table}")
print(f"   Target     : {target_table}")
print(f"   Quarantine : {quarantine_table}")
print(f"   Simulate failure : {simulate}")

# COMMAND ----------

# ── Read Bronze ───────────────────────────────────────────────────────────────
df_bronze    = spark.table(source_table)
bronze_count = df_bronze.count()
print(f"   Rows read from Bronze: {bronze_count:,}")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# ⚠️  FAILURE SIMULATION
# Raises a deliberate ValueError AFTER reading Bronze so the monitoring agent
# can see partial state (Bronze written, Silver not written yet).
# Toggle via job parameter: simulate_failure=true
# ─────────────────────────────────────────────────────────────────────────────
if simulate:
    raise ValueError(
        "💥 SIMULATED FAILURE in silver_transform: "
        "Data validation threshold exceeded — "
        "more than 50% of records failed the transaction_amount > 0 rule. "
        "Aborting write to Silver layer to prevent bad data propagation."
    )

# COMMAND ----------

# ── Quality tagging ───────────────────────────────────────────────────────────
df_tagged = (
    df_bronze
    .withColumn("_rej_amount",
        F.when(F.col("transaction_amount") <= 0, "amount_not_positive").otherwise(None))
    .withColumn("_rej_age",
        F.when(
            F.col("age").isNull() | (F.col("age") < 18) | (F.col("age") > 100),
            "age_out_of_range"
        ).otherwise(None))
    .withColumn("_rejected_reason",
        F.concat_ws(" | ",
            F.coalesce(F.col("_rej_amount"), F.lit("")),
            F.coalesce(F.col("_rej_age"),    F.lit(""))))
    .withColumn("_dq_passed",
        F.col("_rej_amount").isNull() & F.col("_rej_age").isNull())
    .drop("_rej_amount", "_rej_age")
)

passed_df   = df_tagged.filter( F.col("_dq_passed"))
rejected_df = df_tagged.filter(~F.col("_dq_passed"))

# COMMAND ----------

# ── Write quarantine ──────────────────────────────────────────────────────────
rejected_count = rejected_df.count()
if rejected_count > 0:
    (rejected_df.withColumn("_quarantined_at", F.current_timestamp())
     .write.format("delta")
     .mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(quarantine_table))
    print(f"⚠️  {rejected_count:,} rows quarantined → '{quarantine_table}'")

# COMMAND ----------

# ── Transform & write Silver ──────────────────────────────────────────────────
df_silver = (
    passed_df
    .dropDuplicates(["transaction_id"])
    .withColumn("customer_name",
        F.coalesce(F.col("customer_name"), F.lit("Unknown")))
    .withColumn("transaction_amount", F.col("transaction_amount").cast(DoubleType()))
    .withColumn("quantity",           F.col("quantity").cast(IntegerType()))
    .withColumn("discount_pct",       F.col("discount_pct").cast(DoubleType()))
    .withColumn("age",                F.col("age").cast(IntegerType()))
    .withColumn("transaction_date",   F.col("transaction_date").cast(TimestampType()))
    .withColumn("net_amount",
        F.round(F.col("transaction_amount") * (1 - F.col("discount_pct") / 100), 2))
    .withColumn("_processed_at", F.current_timestamp())
    .withColumn("_layer",        F.lit("silver"))
    .drop("_generated_at", "_source_table")
)

(df_silver.write.format("delta")
 .mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(target_table))

# COMMAND ----------

silver_count = spark.table(target_table).count()
print(
    f"✅ Silver transform complete — {silver_count:,} clean rows in '{target_table}' "
    f"| {rejected_count:,} rejected ({rejected_count/bronze_count*100:.1f}%)"
)
