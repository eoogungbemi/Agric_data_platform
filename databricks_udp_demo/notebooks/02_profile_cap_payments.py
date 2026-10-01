# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Profile the source
# MAGIC
# MAGIC Before designing transformations, understand the real source:
# MAGIC columns, nulls, duplicates and data types.

# COMMAND ----------

from pyspark.sql import functions as F

current_catalog = spark.sql("SELECT current_catalog()").first()[0]
raw_table = f"{current_catalog}.raw.cap_beneficiary_payments"
df = spark.table(raw_table)

print(f"Table: {raw_table}")
print(f"Rows: {df.count():,}")
print(f"Columns: {len(df.columns)}")

# COMMAND ----------

print("SOURCE COLUMNS")
for i, c in enumerate(df.columns, start=1):
    print(f"{i:02d}. {c}")

# COMMAND ----------

display(df.limit(25))

# COMMAND ----------

total_rows = df.count()
distinct_rows = df.distinct().count()

print(f"Total rows:      {total_rows:,}")
print(f"Distinct rows:   {distinct_rows:,}")
print(f"Exact duplicates:{total_rows - distinct_rows:,}")

# COMMAND ----------

null_checks = []
for c, dtype in df.dtypes:
    if dtype == "string":
        expr = F.sum(
            F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)
        ).alias(c)
    else:
        expr = F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    null_checks.append(expr)

display(df.select(null_checks))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before notebook 04
# MAGIC Note which source columns represent:
# MAGIC - beneficiary / recipient
# MAGIC - municipality / location
# MAGIC - scheme / measure
# MAGIC - payment amount
