CREATE OR REPLACE TABLE `{{FULL_DATASET}}.dev_alert_queue_top50` AS
SELECT
  event_id,
  dataset_id,
  event_time,
  event_date,
  fraud_score,
  label_fraud,
  queue_id,
  rank_in_queue
FROM `{{FULL_DATASET}}.dev_alert_queue`
WHERE rank_in_queue <= 50;
