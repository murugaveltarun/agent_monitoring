# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1 — Synthetic Data Generation
# MAGIC Generates synthetic customer transaction records and writes to `{catalog}.{schema}.raw_staging`.

# COMMAND ----------

# Widget declarations (overridden by job base_parameters at runtime)
dbutils.widgets.text("catalog", "agent_monitoring")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("num_records", "10000")

catalog     = dbutils.widgets.get("catalog")
schema      = dbutils.widgets.get("schema")
num_records = int(dbutils.widgets.get("num_records"))

print(f"catalog={catalog}  schema={schema}  num_records={num_records}")

# COMMAND ----------

import random, uuid
from datetime import datetime, timedelta
from pyspark.sql.functions import current_timestamp

# ── Constants ────────────────────────────────────────────────────────────────
SEGMENTS        = ["Premium", "Standard", "Basic", "Enterprise"]
CATEGORIES      = ["Electronics", "Food & Beverage", "Travel", "Healthcare",
                   "Entertainment", "Retail", "Automotive"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash"]
STATUSES        = ["Completed", "Pending", "Failed", "Refunded", "Processing"]
COUNTRIES       = ["United States", "United Kingdom", "Germany", "India",
                   "Japan", "Brazil", "Canada", "Australia"]
FIRST_NAMES     = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace",
                   "Henry", "Iris", "Jack", "Karen", "Liam", "Mary", "Noah",
                   "Olivia", "Peter"]
LAST_NAMES      = ["Smith", "Johnson", "Williams", "Brown", "Jones",
                   "Garcia", "Miller", "Davis"]
DEVICE_TYPES    = ["Mobile", "Desktop", "Tablet"]
REFERRALS       = ["Organic", "Paid Ad", "Email", "Social Media", "Referral", "Direct"]

# COMMAND ----------

def _make_record(idx: int) -> dict:
    """One synthetic transaction. ~5 % rows have quality issues."""
    rng          = random.Random(idx)
    has_issue    = (idx % 20 == 0)
    neg_amount   = (idx % 50 == 0)
    country      = rng.choice(COUNTRIES)
    category     = rng.choice(CATEGORIES)
    days_ago     = rng.randint(0, 90)
    txn_date     = datetime.now() - timedelta(days=days_ago,
                                              hours=rng.randint(0, 23),
                                              minutes=rng.randint(0, 59))
    first = rng.choice(FIRST_NAMES)
    last  = rng.choice(LAST_NAMES)
    return {
        "transaction_id":   str(uuid.UUID(int=abs(hash(f"txn_{idx}")))),
        "customer_id":      str(uuid.UUID(int=abs(hash(f"cust_{idx % 2000}")))),
        "customer_name":    f"{first} {last}" if not has_issue else None,
        "email":            f"user{idx % 2000}@example.com",
        "phone":            f"+1-{rng.randint(200,999)}-{rng.randint(100,999)}-{rng.randint(1000,9999)}",
        "age":              rng.randint(18, 80) if not has_issue else rng.choice([-1, 150]),
        "customer_segment": rng.choice(SEGMENTS),
        "transaction_date": txn_date,
        "transaction_amount": (round(rng.uniform(-500.0, -1.0), 2) if neg_amount
                               else round(rng.uniform(5.0, 5000.0), 2)),
        "category":         category,
        "product_name":     f"Product-{category[:3].upper()}-{rng.randint(100, 999)}",
        "quantity":         rng.randint(1, 10),
        "discount_pct":     round(rng.uniform(0.0, 30.0), 2),
        "payment_method":   rng.choice(PAYMENT_METHODS),
        "status":           rng.choice(STATUSES),
        "country":          country,
        "city":             f"City-{rng.randint(1, 50)}",
        "currency":         "USD",
        "is_first_purchase": rng.choice([True, False]),
        "device_type":      rng.choice(DEVICE_TYPES),
        "referral_source":  rng.choice(REFERRALS),
    }

# COMMAND ----------

print(f"🚀 Generating {num_records:,} synthetic records ...")

records = [_make_record(i) for i in range(num_records)]
df = spark.createDataFrame(records).withColumn("_generated_at", current_timestamp())

target_table = f"{catalog}.{schema}.raw_staging"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

count = spark.table(target_table).count()
print(f"✅ Done — {count:,} rows written to '{target_table}'")
