# ARE Modern Data Platform – PySpark Medallion Pipeline
### (Bronze → Silver → Gold)

## 1. Overview

This project implements a PySpark-based Medallion Architecture for the Agriculture & Rural Economy (ARE) sensor and farm datasets.
It demonstrates the approach to:
- Ingest raw CSVs into Bronze Parquet
- Clean, normalise, and apply data quality rules in the Silver layer
- Build analytics-ready datasets in Gold
- Produce moisture trend analysis and streak detection
- Provide lineage and metadata examples

All processing is done locally using PySpark with storage mapped to:

```/Users/dev/PycharmProjects/jup/mnt/agri```

## 2. Project Layout

```/Users/dev/PycharmProjects/jup/mnt/agri```
```
│
├── landing/                     # Raw CSVs
│     ├── farms.csv
│     └── sensor_readings.csv
│
├── bronze/
│     └── parquet/               # Output of Bronze notebook
│           ├── farms/
│           └── sensor_readings/
│
├── silver/
│     └── parquet/               # Output of Silver notebook
│           ├── farms/
│           ├── sensor_readings/
│           ├── soil_moisture/
│           └── temperature/
│
└── gold/
      └── parquet/               # Output of Gold notebook
            ├── daily_farm_stats/
            ├── low_moisture_streaks/
            └── metadata/       # Lineage info
```

### Notebooks (Your real filenames)
```
01_bronze_pyspark.ipynb   →   Landing  →  Bronze
02_silver_pyspark.ipynb   →   Bronze   →  Silver
03_gold_pyspark.ipynb     →   Silver   →  Gold
```
## 3. Tools & Technologies
- PySpark (local mode)
- Python 3.9+
- Java JDK 8/11 (required by Spark)
- Parquet storage
- Optional: Matplotlib (for visualisation)

## 4. Input Data (Landing Layer)
Raw CSVs placed here:
```
/Users/dev/PycharmProjects/jup/mnt/agri/landing
```

```farms.csv``` contains static farm attributes.

```sensor_readings.csv``` Contains timestamped soil moisture and temperature readings from sensors.

Note: reading_ts contains inconsistent formats, e.g.:
```
11/09/2023 03:03
11/09/2023 03:03:00
2023-09-11 03:03:00
- malformed values like 08/03/20023 17:31:00
```

Silver normalises these safely using try_to_timestamp.

## 5. How to Run the Pipeline
### Step 1 — Run 01_bronze_pyspark.ipynb (Landing → Bronze)

Path inside notebook:
```
landing_path = "/Users/dev/PycharmProjects/jup/mnt/agri/landing"
bronze_path  = "/Users/dev/PycharmProjects/jup/mnt/agri/bronze/parquet"
```

What this notebook does:
- Reads both CSVs
- Performs simple type alignment
- Writes Parquet datasets to Bronze:

```
/Users/dev/PycharmProjects/jup/mnt/agri/bronze/parquet/farms
/Users/dev/PycharmProjects/jup/mnt/agri/bronze/parquet/sensor_readings
```
### Step 2 — Run 02_silver_pyspark_nb.ipynb (Bronze → Silver)
Paths used:
```
raw_path    = "/Users/dev/PycharmProjects/jup/mnt/agri/bronze/parquet"
silver_path = "/Users/dev/PycharmProjects/jup/mnt/agri/silver/parquet"
```
Silver responsibilities:

Clean farms:
- Remove duplicates
- Remove records with null farm_id

Clean sensor readings:

- Deduplicate by reading_id
- Remove null farm_ids
- Normalise timestamps using:
```
try_to_timestamp("reading_ts_clean_str", "dd/MM/yyyy HH:mm:ss")
try_to_timestamp("...", "dd/MM/yyyy HH:mm")
try_to_timestamp("...", "yyyy-MM-dd HH:mm:ss")
```
Add parsing status column:
- reading_ts_parse_error = true/false
 
Split into two Silver tables:
- soil_moisture/
- temperature/

Outputs written to:
```/Users/dev/PycharmProjects/jup/mnt/agri/silver/parquet```

## Step 3 — Run 03_gold_pyspark_nb.ipynb (Silver → Gold)
Paths used:
```
silver_path = "/Users/dev/PycharmProjects/jup/mnt/agri/silver/parquet"
gold_path   = "/Users/dev/PycharmProjects/jup/mnt/agri/gold/parquet"
```
Gold responsibilities:

Compute:
1. Daily average soil moisture per farm
2. Daily average temperature per farm
3. Farms with soil moisture < 30% for 3+ consecutive days
(window functions + streak grouping)

Print results:
- Full daily farm stats table
- List of farms with low-moisture streaks

Optional visualisation:
- Soil moisture trend chart for a sample farm

Write Gold outputs:
``` 
/Users/dev/PycharmProjects/jup/mnt/agri/gold/parquet/daily_farm_stats
/Users/dev/PycharmProjects/jup/mnt/agri/gold/parquet/low_moisture_streaks
```
Write lineage metadata:
```
/Users/dev/PycharmProjects/jup/mnt/agri/gold/parquet/_lineage
```

## 6. Assumptions
- Only two sensor types exist: SoilMoisture and Temperature.
- Moisture threshold < 30% is appropriate to indicate crop stress.
- Timestamp formats vary; malformed values are flagged but kept for review.
- Local PySpark is used, but logic is fully cloud-portable (Databricks/Synapse).
- Timezone handling simplified (local system time).

## 7. Dependencies
```
Minimal:
- pyspark
- matplotlib   (optional)

Verify Java is installed:
- java -version

If missing, install a JDK and set:
- export JAVA_HOME=/path/to/jdk
```
## 8. Extensibility 
This local prototype easily scales into an enterprise Lakehouse:
- Add Delta Lake for ACID + time travel
- Add Data Quality rules using Delta Expectations
- Add ingestion orchestration via ADF or Databricks Jobs
- Introduce CI/CD & automated tests
- Add a Data Catalog (Purview / Unity Catalog)
- Add streaming ingestion for sensor data (Kafka/EventHub → Spark Structured Streaming)
