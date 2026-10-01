# UDP Databricks Demo

This folder is a guided Databricks Free Edition demonstration of the Unified Data Platform pattern:

**Public source data → Raw → Cleansed → Curated → Farm Support 360 → Dashboard / Lineage**

The demo uses public Scottish Government CAP beneficiary-payment data. Do not use live ARE, ARP, customer, holding, inspection, payment or other sensitive operational data in Databricks Free Edition.

## Build sequence

Run the notebooks in order:

1. `00_setup.py` — create Raw, Cleansed and Curated schemas
2. `01_ingest_cap_payments.py` — land the public CAP CSV as a Raw Delta table
3. `02_profile_cap_payments.py` — inspect the real structure and quality of the source
4. `03_clean_cap_payments.py` — standardise structure and create the Cleansed table
5. `04_build_farm_support_360.py` — create a beneficiary-level curated data product
6. `05_demo_walkthrough.py` — demonstrate Raw → Cleansed → Curated → Insight

## Public dataset

Scottish Government publication:
https://www.gov.scot/publications/common-agricultural-policy-legacy-schemes-beneficiary-payments/

Download the workbook and save the data worksheet as:

`cap_beneficiary_payments.csv`

For Free Edition, the simplest ingestion route is Databricks **+ New → Add data**, then create:

`<your_catalog>.raw.cap_beneficiary_payments`

## Target structure

```text
<current_catalog>
├── raw
│   └── cap_beneficiary_payments
├── cleansed
│   └── cap_beneficiary_payments
└── curated
    └── farm_support_360
```

## Important design point

The public dataset supports a **Farm Support 360 / Beneficiary 360** demonstration. It is not a literal Holding 360 because public data does not expose the operational identifiers needed to connect customer, holding, parcel, inspection and livestock records.

The purpose of this demo is to prove the platform pattern, not to pretend public data contains operational relationships it does not contain.
