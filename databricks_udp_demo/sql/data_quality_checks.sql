-- UDP demo data-quality checks
-- Replace <catalog> with the catalog shown by notebook 00_setup.py.

-- Raw vs Cleansed row counts
SELECT 'raw' AS layer, COUNT(*) AS row_count
FROM <catalog>.raw.cap_beneficiary_payments
UNION ALL
SELECT 'cleansed' AS layer, COUNT(*) AS row_count
FROM <catalog>.cleansed.cap_beneficiary_payments;

-- Missing fields in Cleansed
SELECT
  _udp_missing_field_count,
  COUNT(*) AS records
FROM <catalog>.cleansed.cap_beneficiary_payments
GROUP BY _udp_missing_field_count
ORDER BY _udp_missing_field_count;

-- Curated quality status
SELECT
  data_quality_status,
  COUNT(*) AS beneficiary_records,
  SUM(total_support_gbp) AS total_support_gbp
FROM <catalog>.curated.farm_support_360
GROUP BY data_quality_status
ORDER BY beneficiary_records DESC;
