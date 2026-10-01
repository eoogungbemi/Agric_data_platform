# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest CAP beneficiary payments
# MAGIC
# MAGIC Recommended Free Edition route:
# MAGIC 1. Download the public Scottish Government CAP beneficiary workbook.
# MAGIC 2. Save the data worksheet as CSV.
# MAGIC 3. Use **+ New → Add data** in Databricks.
# MAGIC 4. Create the table in the **raw** schema as **cap_beneficiary_payments**.
# MAGIC
# MAGIC This notebook validates that the Raw table exists and lets you inspect it.

# COMMAND ----------

current_catalog = spark.sql("SELECT current_catalog()").first()[0]
raw_table = f"{current_catalog}.raw.cap_beneficiary_payments"
print("Expected Raw table:", raw_table)

# COMMAND ----------

try:
    df = spark.table(raw_table)
except Exception as exc:
    raise ValueError(
        f"{raw_table} does not exist yet. Upload the CSV using Add data, "
        "choose the raw schema, and name the table cap_beneficiary_payments."
    ) from exc

# COMMAND ----------

print(f"Rows: {df.count():,}")
print(f"Columns: {len(df.columns)}")
display(df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC **Stakeholder explanation:**  
# MAGIC "This is the source information as received. At the Raw layer we preserve it before applying business transformations."
