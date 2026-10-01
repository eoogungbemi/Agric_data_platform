# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Clean and standardise CAP payments

# COMMAND ----------

import re
from pyspark.sql import functions as F

current_catalog = spark.sql("SELECT current_catalog()").first()[0]
raw_table = f"{current_catalog}.raw.cap_beneficiary_payments"
clean_table = f"{current_catalog}.cleansed.cap_beneficiary_payments"

df = spark.table(raw_table)

# COMMAND ----------

def safe_name(name):
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    return name or "unnamed_column"

seen = {}
for old in df.columns:
    base = safe_name(old)
    n = seen.get(base, 0)
    seen[base] = n + 1
    new = base if n == 0 else f"{base}_{n+1}"
    df = df.withColumnRenamed(old, new)

print("Standardised columns:")
for c in df.columns:
    print(" -", c)

# COMMAND ----------

dtype_map = dict(df.dtypes)
for c in df.columns:
    if dtype_map[c] == "string":
        df = df.withColumn(c, F.trim(F.col(c)))

before = df.count()
df = df.dropDuplicates()
after = df.count()

print(f"Exact duplicates removed: {before - after:,}")

# COMMAND ----------

business_columns = list(df.columns)

missing_expr = None
for c in business_columns:
    check = F.when(
        F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""),
        1
    ).otherwise(0)
    missing_expr = check if missing_expr is None else missing_expr + check

df = (
    df
    .withColumn("_udp_missing_field_count", missing_expr)
    .withColumn("_udp_source", F.lit("Scottish Government public CAP beneficiary publication"))
    .withColumn("_udp_layer", F.lit("cleansed"))
    .withColumn("_udp_processed_at", F.current_timestamp())
)

# COMMAND ----------

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(clean_table)
)

print("Created:", clean_table)
display(spark.table(clean_table).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC **Stakeholder explanation:**  
# MAGIC "We have not changed the Raw source. We have created a controlled, standardised representation and made quality issues measurable."
