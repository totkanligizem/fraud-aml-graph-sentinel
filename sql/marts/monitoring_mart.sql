DROP TABLE IF EXISTS monitoring_mart;

CREATE TABLE monitoring_mart AS
SELECT
  dataset_id,
  date(event_time) AS event_date,
  COUNT(*) AS txn_count,
  AVG(amount) AS avg_amount,
  SUM(CASE WHEN label_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
  SUM(CASE WHEN label_aml = 1 THEN 1 ELSE 0 END) AS aml_count
FROM transaction_mart
GROUP BY 1, 2;
