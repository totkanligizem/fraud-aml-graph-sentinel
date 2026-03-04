CREATE OR REPLACE VIEW `{{FULL_DATASET}}.dev_exec_analyst_surface` AS
WITH ranked AS (
  SELECT
    a.*,
    ROW_NUMBER() OVER (
      PARTITION BY a.queue_id
      ORDER BY a.created_at_utc DESC, a.run_id DESC, a.dataset_id ASC
    ) AS recency_rank
  FROM `{{FULL_DATASET}}.dev_analyst_case_summary` AS a
)
SELECT
  a.run_id,
  a.created_at_utc AS analyst_generated_at,
  a.project_id,
  a.location,
  a.model,
  a.dataset_id,
  a.queue_id,
  a.event_date,
  q.queue_rows,
  q.avg_queue_score,
  q.max_queue_score,
  q.positive_rows,
  q.top50_rows,
  q.top50_true_fraud_rows,
  q.top50_precision,
  q.dataset_rank,
  q.overall_rank,
  a.overall_priority,
  a.fraud_risk,
  a.aml_risk,
  a.network_risk,
  a.response_chars,
  a.observed_signal_count,
  a.hypothesis_count,
  a.action_count,
  COALESCE(ARRAY_LENGTH(a.evidence_gaps), 0) AS evidence_gap_count,
  a.case_overview,
  a.observed_signals,
  a.recommended_actions,
  a.evidence_gaps,
  a.investigation_hypotheses
FROM ranked AS a
LEFT JOIN `{{FULL_DATASET}}.dev_exec_queue_watchlist` AS q
  ON a.dataset_id = q.dataset_id
 AND a.queue_id = q.queue_id
WHERE a.recency_rank = 1;
