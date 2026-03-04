CREATE OR REPLACE VIEW `{{FULL_DATASET}}.dev_exec_dataset_surface` AS
WITH tm AS (
  SELECT
    dataset_id,
    COUNT(*) AS transaction_rows,
    SUM(COALESCE(label_fraud, 0)) AS fraud_rows,
    SUM(COALESCE(label_aml, 0)) AS aml_rows,
    AVG(amount) AS avg_amount,
    MIN(event_date) AS first_event_date,
    MAX(event_date) AS last_event_date
  FROM `{{FULL_DATASET}}.dev_transaction_mart`
  GROUP BY dataset_id
),
fs AS (
  SELECT
    dataset_id,
    COUNT(*) AS scored_rows,
    AVG(fraud_score) AS avg_fraud_score,
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(95)] AS p95_fraud_score,
    SUM(CASE WHEN fraud_score >= 0.8 THEN 1 ELSE 0 END) AS high_risk_scored_rows
  FROM `{{FULL_DATASET}}.dev_fraud_scores`
  GROUP BY dataset_id
),
aq AS (
  SELECT
    dataset_id,
    COUNT(*) AS queue_rows,
    COUNT(DISTINCT queue_id) AS queue_count,
    AVG(fraud_score) AS avg_queue_score,
    MAX(fraud_score) AS max_queue_score,
    SUM(COALESCE(label_fraud, 0)) AS queue_positive_rows
  FROM `{{FULL_DATASET}}.dev_alert_queue`
  GROUP BY dataset_id
),
top50 AS (
  SELECT
    dataset_id,
    COUNT(*) AS top50_rows,
    SUM(COALESCE(label_fraud, 0)) AS top50_true_fraud_rows,
    SAFE_DIVIDE(SUM(COALESCE(label_fraud, 0)), NULLIF(COUNT(*), 0)) AS top50_precision_micro
  FROM `{{FULL_DATASET}}.dev_alert_queue`
  WHERE rank_in_queue <= 50
  GROUP BY dataset_id
)
SELECT
  tm.dataset_id,
  tm.transaction_rows,
  tm.fraud_rows,
  tm.aml_rows,
  tm.avg_amount,
  tm.first_event_date,
  tm.last_event_date,
  COALESCE(fs.scored_rows, 0) AS scored_rows,
  COALESCE(fs.avg_fraud_score, 0.0) AS avg_fraud_score,
  COALESCE(fs.p95_fraud_score, 0.0) AS p95_fraud_score,
  COALESCE(fs.high_risk_scored_rows, 0) AS high_risk_scored_rows,
  COALESCE(aq.queue_rows, 0) AS queue_rows,
  COALESCE(aq.queue_count, 0) AS queue_count,
  COALESCE(aq.avg_queue_score, 0.0) AS avg_queue_score,
  COALESCE(aq.max_queue_score, 0.0) AS max_queue_score,
  COALESCE(aq.queue_positive_rows, 0) AS queue_positive_rows,
  COALESCE(top50.top50_rows, 0) AS top50_rows,
  COALESCE(top50.top50_true_fraud_rows, 0) AS top50_true_fraud_rows,
  COALESCE(top50.top50_precision_micro, 0.0) AS top50_precision_micro,
  SAFE_DIVIDE(tm.transaction_rows, NULLIF(SUM(tm.transaction_rows) OVER (), 0)) AS share_of_volume,
  SAFE_DIVIDE(COALESCE(fs.scored_rows, 0), NULLIF(tm.transaction_rows, 0)) AS scoring_coverage,
  SAFE_DIVIDE(tm.fraud_rows, NULLIF(tm.transaction_rows, 0)) AS fraud_rate,
  SAFE_DIVIDE(tm.aml_rows, NULLIF(tm.transaction_rows, 0)) AS aml_rate,
  SAFE_DIVIDE(COALESCE(fs.high_risk_scored_rows, 0), NULLIF(COALESCE(fs.scored_rows, 0), 0)) AS high_risk_scored_share
FROM tm
LEFT JOIN fs
  ON tm.dataset_id = fs.dataset_id
LEFT JOIN aq
  ON tm.dataset_id = aq.dataset_id
LEFT JOIN top50
  ON tm.dataset_id = top50.dataset_id;
