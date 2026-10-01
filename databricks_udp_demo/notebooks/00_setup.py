# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — UDP demo setup
# MAGIC
# MAGIC Creates the three logical data layers used in this demonstration:
# MAGIC **Raw → Cleansed → Curated**.

# COMMAND ----------

current_catalog = spark.sql("SELECT current_catalog()").first()[0]
print(f"Current catalog: {current_catalog}")

# COMMAND ----------

for schema in ["raw", "cleansed", "curated"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS \`{current_catalog}\`.\`{schema}\`")
    print(f"Ready: {current_catalog}.{schema}")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN \`{current_catalog}\`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you have just built
# MAGIC
# MAGIC - **Raw** preserves source information.
# MAGIC - **Cleansed** applies standardisation and quality controls.
# MAGIC - **Curated** publishes business-friendly reusable data products.
