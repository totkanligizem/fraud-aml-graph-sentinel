CREATE OR REPLACE VIEW `{{FULL_DATASET}}.dev_exec_queue_watchlist` AS
WITH queue_base AS (
  SELECT
    dataset_id,
    queue_id,
    event_date,
    COUNT(*) AS queue_rows,
    AVG(fraud_score) AS avg_queue_score,
    MAX(fraud_score) AS max_queue_score,
    SUM(COALESCE(label_fraud, 0)) AS positive_rows
  FROM `{{FULL_DATASET}}.dev_alert_queue`
  GROUP BY dataset_id, queue_id, event_date
),
queue_top50 AS (
  SELECT
    dataset_id,
    queue_id,
    event_date,
    COUNT(*) AS top50_rows,
    SUM(COALESCE(label_fraud, 0)) AS top50_true_fraud_rows,
    SAFE_DIVIDE(SUM(COALESCE(label_fraud, 0)), NULLIF(COUNT(*), 0)) AS top50_precision
  FROM `{{FULL_DATASET}}.dev_alert_queue`
  WHERE rank_in_queue <= 50
  GROUP BY dataset_id, queue_id, event_date
)
SELECT
  qb.dataset_id,
  qb.queue_id,
  qb.event_date,
  qb.queue_rows,
  qb.avg_queue_score,
  qb.max_queue_score,
  qb.positive_rows,
  COALESCE(q50.top50_rows, 0) AS top50_rows,
  COALESCE(q50.top50_true_fraud_rows, 0) AS top50_true_fraud_rows,
  COALESCE(q50.top50_precision, 0.0) AS top50_precision,
  DENSE_RANK() OVER (
    PARTITION BY qb.dataset_id
    ORDER BY qb.avg_queue_score DESC, qb.positive_rows DESC, qb.queue_rows DESC, qb.queue_id ASC
  ) AS dataset_rank,
  DENSE_RANK() OVER (
    ORDER BY qb.avg_queue_score DESC, qb.positive_rows DESC, qb.queue_rows DESC, qb.queue_id ASC
  ) AS overall_rank
FROM queue_base AS qb
LEFT JOIN queue_top50 AS q50
  ON qb.dataset_id = q50.dataset_id
 AND qb.queue_id = q50.queue_id
 AND qb.event_date = q50.event_date;
