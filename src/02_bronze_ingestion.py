# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2 — Bronze Ingestion
# MAGIC Reads `raw_staging` → enriches with metadata → writes `bronze_raw_transactions`.

# COMMAND ----------

dbutils.widgets.text("catalog", "agent_monitoring")
dbutils.widgets.text("schema", "default")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

print(f"catalog={catalog}  schema={schema}")

# COMMAND ----------

import uuid
from pyspark.sql.functions import current_timestamp, lit

source_table = f"{catalog}.{schema}.raw_staging"
target_table = f"{catalog}.{schema}.bronze_raw_transactions"
batch_id     = str(uuid.uuid4())

print(f"🚀 Bronze ingestion starting")
print(f"   Source : {source_table}")
print(f"   Target : {target_table}")
print(f"   Batch  : {batch_id}")

# COMMAND ----------

df_raw       = spark.table(source_table)
source_count = df_raw.count()
print(f"   Rows read from staging: {source_count:,}")

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("_ingested_at",  current_timestamp())
    .withColumn("_source_table", lit(source_table))
    .withColumn("_batch_id",     lit(batch_id))
    .withColumn("_layer",        lit("bronze"))
)

(
    df_bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

# COMMAND ----------

written_count = spark.table(target_table).count()
print(f"✅ Bronze ingestion complete — {written_count:,} rows in '{target_table}' (batch: {batch_id})")

if written_count != source_count:
    raise RuntimeError(
        f"Row count mismatch! Expected {source_count:,}, got {written_count:,}"
    )
