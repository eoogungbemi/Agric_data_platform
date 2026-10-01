# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Build Farm Support 360
# MAGIC
# MAGIC This creates a beneficiary-level curated data product.
# MAGIC Run notebook 02 first so you know the actual cleansed column names.

# COMMAND ----------

from pyspark.sql import functions as F

current_catalog = spark.sql("SELECT current_catalog()").first()[0]
clean_table = f"{current_catalog}.cleansed.cap_beneficiary_payments"
curated_table = f"{current_catalog}.curated.farm_support_360"
df = spark.table(clean_table)

print("Available columns:")
for c in df.columns:
    print(" -", c)

# COMMAND ----------

dbutils.widgets.text("beneficiary_column", "", "Beneficiary column")
dbutils.widgets.text("municipality_column", "", "Municipality column")
dbutils.widgets.text("scheme_column", "", "Scheme column")
dbutils.widgets.text("payment_amount_column", "", "Payment amount column")

beneficiary_col = dbutils.widgets.get("beneficiary_column").strip()
municipality_col = dbutils.widgets.get("municipality_column").strip()
scheme_col = dbutils.widgets.get("scheme_column").strip()
payment_col = dbutils.widgets.get("payment_amount_column").strip()

mapping = {
    "beneficiary": beneficiary_col,
    "municipality": municipality_col,
    "scheme": scheme_col,
    "payment_amount": payment_col,
}

missing = [k for k, v in mapping.items() if not v]
if missing:
    raise ValueError("Complete these widgets first: " + ", ".join(missing))

invalid = [v for v in mapping.values() if v not in df.columns]
if invalid:
    raise ValueError("Column(s) not found: " + ", ".join(invalid))

print("Mapping validated:", mapping)

# COMMAND ----------

payment_as_number = F.regexp_replace(
    F.col(payment_col).cast("string"),
    r"[^0-9.\-]",
    ""
).cast("decimal(18,2)")

prepared = (
    df
    .withColumn("_beneficiary", F.trim(F.col(beneficiary_col).cast("string")))
    .withColumn("_municipality", F.trim(F.col(municipality_col).cast("string")))
    .withColumn("_scheme", F.trim(F.col(scheme_col).cast("string")))
    .withColumn("_payment_amount", payment_as_number)
    .withColumn(
        "_publication_status",
        F.when(F.col(beneficiary_col).isNull(), "NAME WITHHELD / NOT PUBLISHED")
         .when(F.col(municipality_col).isNull(), "REVIEW LOCATION")
         .when(payment_as_number.isNull(), "REVIEW PAYMENT VALUE")
         .otherwise("OK")
    )
)

# COMMAND ----------

prepared = prepared.withColumn(
    "_beneficiary_group",
    F.when(
        F.col("_beneficiary").isNull() | (F.col("_beneficiary") == ""),
        F.concat(F.lit("WITHHELD|"), F.coalesce(F.col("_municipality"), F.lit("UNKNOWN")))
    ).otherwise(F.col("_beneficiary"))
)

farm360 = (
    prepared
    .groupBy("_beneficiary_group", "_beneficiary", "_municipality")
    .agg(
        F.count("*").alias("payment_records"),
        F.countDistinct("_scheme").alias("number_of_schemes"),
        F.collect_set("_scheme").alias("schemes"),
        F.sum("_payment_amount").alias("total_support_gbp"),
        F.collect_set("_publication_status").alias("_statuses")
    )
    .withColumnRenamed("_beneficiary", "beneficiary_name")
    .withColumnRenamed("_municipality", "municipality")
    .withColumn(
        "beneficiary_display_name",
        F.when(
            F.col("beneficiary_name").isNull() | (F.col("beneficiary_name") == ""),
            F.lit("Name withheld / not published")
        ).otherwise(F.col("beneficiary_name"))
    )
    .withColumn(
        "data_quality_status",
        F.when(F.array_contains("_statuses", "REVIEW PAYMENT VALUE"), "REVIEW")
         .when(F.array_contains("_statuses", "REVIEW LOCATION"), "REVIEW")
         .when(F.array_contains("_statuses", "NAME WITHHELD / NOT PUBLISHED"), "PUBLISHED WITH WITHHELD NAME")
         .otherwise("OK")
    )
    .withColumn("_udp_layer", F.lit("curated"))
    .withColumn("_udp_created_at", F.current_timestamp())
    .drop("_beneficiary_group", "_statuses")
)

# COMMAND ----------

(
    farm360.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(curated_table)
)

print("Created:", curated_table)
display(
    spark.table(curated_table)
         .orderBy(F.col("total_support_gbp").desc_nulls_last())
         .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Stakeholder explanation:**  
# MAGIC "The curated layer organises source data around a reusable business need. Consumers no longer need to repeat the same joining, cleaning and aggregation logic."
