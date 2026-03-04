CREATE OR REPLACE TABLE `{{FULL_DATASET}}.dev_risk_dashboard_daily` AS
WITH score_daily AS (
  SELECT
    dataset_id,
    DATE(event_time) AS event_date,
    COUNT(*) AS scored_event_count,
    AVG(fraud_score) AS avg_fraud_score,
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(95)] AS p95_fraud_score
  FROM `{{FULL_DATASET}}.dev_fraud_scores`
  GROUP BY dataset_id, event_date
),
queue_top50_daily AS (
  SELECT
    dataset_id,
    event_date,
    COUNT(*) AS top50_alert_count,
    SUM(COALESCE(label_fraud, 0)) AS top50_true_fraud_count
  FROM `{{FULL_DATASET}}.dev_alert_queue`
  WHERE rank_in_queue <= 50
  GROUP BY dataset_id, event_date
)
SELECT
  m.event_date,
  m.dataset_id,
  m.txn_count,
  m.avg_amount,
  m.fraud_count,
  m.aml_count,
  SAFE_DIVIDE(m.fraud_count, NULLIF(m.txn_count, 0)) AS fraud_rate,
  COALESCE(s.scored_event_count, 0) AS scored_event_count,
  COALESCE(s.avg_fraud_score, 0.0) AS avg_fraud_score,
  COALESCE(s.p95_fraud_score, 0.0) AS p95_fraud_score,
  COALESCE(q.top50_alert_count, 0) AS top50_alert_count,
  COALESCE(q.top50_true_fraud_count, 0) AS top50_true_fraud_count,
  SAFE_DIVIDE(COALESCE(q.top50_true_fraud_count, 0), NULLIF(COALESCE(q.top50_alert_count, 0), 0)) AS top50_precision
FROM `{{FULL_DATASET}}.dev_monitoring_mart` AS m
LEFT JOIN score_daily AS s
  ON m.dataset_id = s.dataset_id
 AND m.event_date = s.event_date
LEFT JOIN queue_top50_daily AS q
  ON m.dataset_id = q.dataset_id
 AND m.event_date = q.event_date;
