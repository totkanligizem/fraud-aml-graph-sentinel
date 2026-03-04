SELECT
  dataset_id,
  COUNT(*) AS scored_rows,
  SUM(COALESCE(label_fraud, 0)) AS true_fraud_rows,
  SAFE_DIVIDE(SUM(COALESCE(label_fraud, 0)), NULLIF(COUNT(*), 0)) AS true_fraud_rate,
  AVG(fraud_score) AS avg_fraud_score,
  APPROX_QUANTILES(fraud_score, 100)[OFFSET(95)] AS p95_fraud_score
FROM `{{FULL_DATASET}}.dev_fraud_scores`
GROUP BY dataset_id
ORDER BY dataset_id;
