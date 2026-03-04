WITH null_event_id AS (
  SELECT COUNT(*) AS value
  FROM `{{FULL_DATASET}}.dev_transaction_mart`
  WHERE event_id IS NULL OR event_id = ''
),
duplicate_event_id AS (
  SELECT COUNT(*) AS value
  FROM (
    SELECT event_id, COUNT(*) AS c
    FROM `{{FULL_DATASET}}.dev_transaction_mart`
    GROUP BY event_id
    HAVING COUNT(*) > 1
  )
),
null_source_event_id AS (
  SELECT COUNT(*) AS value
  FROM `{{FULL_DATASET}}.dev_transaction_mart`
  WHERE source_event_id IS NULL OR source_event_id = ''
),
invalid_label_aml AS (
  SELECT COUNT(*) AS value
  FROM `{{FULL_DATASET}}.dev_transaction_mart`
  WHERE label_aml IS NOT NULL AND label_aml NOT IN (0, 1)
),
invalid_score_range AS (
  SELECT COUNT(*) AS value
  FROM `{{FULL_DATASET}}.dev_fraud_scores`
  WHERE fraud_score < 0 OR fraud_score > 1 OR fraud_score IS NULL
),
invalid_queue_rank AS (
  SELECT COUNT(*) AS value
  FROM `{{FULL_DATASET}}.dev_alert_queue`
  WHERE rank_in_queue IS NULL OR rank_in_queue < 1
),
missing_queue_id AS (
  SELECT COUNT(*) AS value
  FROM `{{FULL_DATASET}}.dev_alert_queue`
  WHERE queue_id IS NULL OR queue_id = ''
)
SELECT 'null_event_id_transaction_mart' AS metric_name, value AS metric_value FROM null_event_id
UNION ALL
SELECT 'duplicate_event_id_transaction_mart' AS metric_name, value AS metric_value FROM duplicate_event_id
UNION ALL
SELECT 'invalid_label_aml_transaction_mart' AS metric_name, value AS metric_value FROM invalid_label_aml
UNION ALL
SELECT 'invalid_fraud_score_range' AS metric_name, value AS metric_value FROM invalid_score_range
UNION ALL
SELECT 'invalid_queue_rank' AS metric_name, value AS metric_value FROM invalid_queue_rank
UNION ALL
SELECT 'missing_queue_id' AS metric_name, value AS metric_value FROM missing_queue_id
UNION ALL
SELECT 'null_source_event_id_transaction_mart' AS metric_name, value AS metric_value FROM null_source_event_id
ORDER BY metric_name;
