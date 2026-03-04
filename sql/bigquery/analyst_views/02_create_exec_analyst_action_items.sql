CREATE OR REPLACE VIEW `{{FULL_DATASET}}.dev_exec_analyst_action_items` AS
SELECT
  s.run_id,
  s.analyst_generated_at,
  s.project_id,
  s.location,
  s.model,
  s.dataset_id,
  s.queue_id,
  s.event_date,
  s.overall_priority,
  s.fraud_risk,
  s.aml_risk,
  s.network_risk,
  s.avg_queue_score,
  s.top50_precision,
  s.overall_rank,
  action_offset + 1 AS action_rank,
  action_text AS recommended_action
FROM `{{FULL_DATASET}}.dev_exec_analyst_surface` AS s,
UNNEST(s.recommended_actions) AS action_text WITH OFFSET AS action_offset;
