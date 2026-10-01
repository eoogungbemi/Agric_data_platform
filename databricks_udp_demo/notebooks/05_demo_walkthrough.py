# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Stakeholder demo walkthrough
# MAGIC
# MAGIC Use this after notebooks 00–04 have completed.

# COMMAND ----------

current_catalog = spark.sql("SELECT current_catalog()").first()[0]
raw_table = f"{current_catalog}.raw.cap_beneficiary_payments"
clean_table = f"{current_catalog}.cleansed.cap_beneficiary_payments"
curated_table = f"{current_catalog}.curated.farm_support_360"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Raw
# MAGIC "This is the information as received from the source. We preserve it before applying business transformations."

# COMMAND ----------

display(spark.table(raw_table).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Cleansed
# MAGIC "We standardise structure, remove exact duplicates and make quality issues measurable."

# COMMAND ----------

display(spark.table(clean_table).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Curated Farm Support 360
# MAGIC "We reorganise data around a reusable business need rather than forcing every consumer to repeat the transformation."

# COMMAND ----------

display(
    spark.table(curated_table)
         .orderBy("total_support_gbp", ascending=False)
         .limit(30)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Quality

# COMMAND ----------

display(spark.sql(f"""
SELECT
    data_quality_status,
    COUNT(*) AS beneficiary_records,
    SUM(total_support_gbp) AS total_support_gbp
FROM {curated_table}
GROUP BY data_quality_status
ORDER BY beneficiary_records DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Reuse / insight

# COMMAND ----------

display(spark.sql(f"""
SELECT
    COALESCE(municipality, 'Not published / unknown') AS municipality,
    COUNT(*) AS beneficiary_records,
    SUM(total_support_gbp) AS total_support_gbp
FROM {curated_table}
GROUP BY COALESCE(municipality, 'Not published / unknown')
ORDER BY total_support_gbp DESC
LIMIT 25
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Governance and lineage
# MAGIC Open **Catalog Explorer → curated → farm_support_360**.
# MAGIC
# MAGIC Show the table metadata and lineage where available.
# MAGIC
# MAGIC **Close with:**  
# MAGIC "The dashboard is only the visible end. The platform gives us a controlled path from source data to trusted, reusable information."
