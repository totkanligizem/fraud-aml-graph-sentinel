SELECT
  queue_id,
  COUNT(*) AS alert_rows_top50,
  SUM(COALESCE(label_fraud, 0)) AS true_fraud_rows_top50,
  SAFE_DIVIDE(SUM(COALESCE(label_fraud, 0)), NULLIF(COUNT(*), 0)) AS precision_at_50,
  AVG(fraud_score) AS avg_fraud_score_top50
FROM `{{FULL_DATASET}}.dev_alert_queue_top50`
GROUP BY queue_id
ORDER BY precision_at_50 DESC, true_fraud_rows_top50 DESC, queue_id
LIMIT 200;
