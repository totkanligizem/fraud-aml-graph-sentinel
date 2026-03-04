CREATE OR REPLACE VIEW `{{FULL_DATASET}}.dev_exec_daily_surface` AS
WITH monitoring_dataset AS (
  SELECT
    dataset_id AS lens_id,
    event_date,
    txn_count,
    avg_amount,
    fraud_count,
    aml_count
  FROM `{{FULL_DATASET}}.dev_monitoring_mart`
),
monitoring_overview AS (
  SELECT
    'overview' AS lens_id,
    event_date,
    SUM(txn_count) AS txn_count,
    AVG(avg_amount) AS avg_amount,
    SUM(fraud_count) AS fraud_count,
    SUM(aml_count) AS aml_count
  FROM `{{FULL_DATASET}}.dev_monitoring_mart`
  GROUP BY event_date
),
monitoring_all AS (
  SELECT * FROM monitoring_dataset
  UNION ALL
  SELECT * FROM monitoring_overview
),
score_dataset AS (
  SELECT
    dataset_id AS lens_id,
    DATE(event_time) AS event_date,
    COUNT(*) AS scored_event_count,
    AVG(fraud_score) AS avg_fraud_score,
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(95)] AS p95_fraud_score,
    SUM(CASE WHEN fraud_score >= 0.8 THEN 1 ELSE 0 END) AS high_risk_scored_rows
  FROM `{{FULL_DATASET}}.dev_fraud_scores`
  GROUP BY lens_id, event_date
),
score_overview AS (
  SELECT
    'overview' AS lens_id,
    DATE(event_time) AS event_date,
    COUNT(*) AS scored_event_count,
    AVG(fraud_score) AS avg_fraud_score,
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(95)] AS p95_fraud_score,
    SUM(CASE WHEN fraud_score >= 0.8 THEN 1 ELSE 0 END) AS high_risk_scored_rows
  FROM `{{FULL_DATASET}}.dev_fraud_scores`
  GROUP BY event_date
),
score_all AS (
  SELECT * FROM score_dataset
  UNION ALL
  SELECT * FROM score_overview
),
queue_dataset AS (
  SELECT
    dataset_id AS lens_id,
    event_date,
    COUNT(*) AS queue_rows,
    COUNTIF(rank_in_queue <= 50) AS top50_rows,
    SUM(CASE WHEN rank_in_queue <= 50 THEN COALESCE(label_fraud, 0) ELSE 0 END) AS top50_true_fraud_rows,
    AVG(fraud_score) AS avg_queue_score,
    MAX(fraud_score) AS max_queue_score
  FROM `{{FULL_DATASET}}.dev_alert_queue`
  GROUP BY lens_id, event_date
),
queue_overview AS (
  SELECT
    'overview' AS lens_id,
    event_date,
    COUNT(*) AS queue_rows,
    COUNTIF(rank_in_queue <= 50) AS top50_rows,
    SUM(CASE WHEN rank_in_queue <= 50 THEN COALESCE(label_fraud, 0) ELSE 0 END) AS top50_true_fraud_rows,
    AVG(fraud_score) AS avg_queue_score,
    MAX(fraud_score) AS max_queue_score
  FROM `{{FULL_DATASET}}.dev_alert_queue`
  GROUP BY event_date
),
queue_all AS (
  SELECT * FROM queue_dataset
  UNION ALL
  SELECT * FROM queue_overview
)
SELECT
  m.lens_id,
  m.event_date,
  m.txn_count,
  m.avg_amount,
  m.fraud_count,
  m.aml_count,
  COALESCE(s.scored_event_count, 0) AS scored_event_count,
  COALESCE(s.avg_fraud_score, 0.0) AS avg_fraud_score,
  COALESCE(s.p95_fraud_score, 0.0) AS p95_fraud_score,
  COALESCE(s.high_risk_scored_rows, 0) AS high_risk_scored_rows,
  COALESCE(q.queue_rows, 0) AS queue_rows,
  COALESCE(q.top50_rows, 0) AS top50_rows,
  COALESCE(q.top50_true_fraud_rows, 0) AS top50_true_fraud_rows,
  COALESCE(q.avg_queue_score, 0.0) AS avg_queue_score,
  COALESCE(q.max_queue_score, 0.0) AS max_queue_score,
  SAFE_DIVIDE(m.fraud_count, NULLIF(m.txn_count, 0)) AS fraud_rate,
  SAFE_DIVIDE(m.aml_count, NULLIF(m.txn_count, 0)) AS aml_rate,
  SAFE_DIVIDE(COALESCE(q.top50_true_fraud_rows, 0), NULLIF(COALESCE(q.top50_rows, 0), 0)) AS top50_precision,
  SAFE_DIVIDE(COALESCE(s.high_risk_scored_rows, 0), NULLIF(COALESCE(s.scored_event_count, 0), 0)) AS high_risk_scored_share
FROM monitoring_all AS m
LEFT JOIN score_all AS s
  ON m.lens_id = s.lens_id
 AND m.event_date = s.event_date
LEFT JOIN queue_all AS q
  ON m.lens_id = q.lens_id
 AND m.event_date = q.event_date;
