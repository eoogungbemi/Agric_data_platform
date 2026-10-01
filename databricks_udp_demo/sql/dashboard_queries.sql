-- UDP demo dashboard queries
-- Replace <catalog> with the catalog shown by notebook 00_setup.py.

-- Total published support
SELECT SUM(total_support_gbp) AS total_support_gbp
FROM <catalog>.curated.farm_support_360;

-- Beneficiary-level records
SELECT COUNT(*) AS beneficiary_records
FROM <catalog>.curated.farm_support_360;

-- Support by municipality
SELECT
  COALESCE(municipality, 'Not published / unknown') AS municipality,
  SUM(total_support_gbp) AS total_support_gbp
FROM <catalog>.curated.farm_support_360
GROUP BY COALESCE(municipality, 'Not published / unknown')
ORDER BY total_support_gbp DESC;

-- Quality status
SELECT
  data_quality_status,
  COUNT(*) AS beneficiary_records,
  SUM(total_support_gbp) AS total_support_gbp
FROM <catalog>.curated.farm_support_360
GROUP BY data_quality_status
ORDER BY beneficiary_records DESC;

-- Top 25
SELECT
  beneficiary_display_name,
  municipality,
  number_of_schemes,
  payment_records,
  total_support_gbp,
  data_quality_status
FROM <catalog>.curated.farm_support_360
ORDER BY total_support_gbp DESC
LIMIT 25;
