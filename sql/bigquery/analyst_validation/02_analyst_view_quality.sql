WITH surface AS (
  SELECT * FROM `{{FULL_DATASET}}.dev_exec_analyst_surface`
),
actions AS (
  SELECT * FROM `{{FULL_DATASET}}.dev_exec_analyst_action_items`
)
SELECT 'invalid_overall_priority' AS check_name, COUNT(*) AS defect_count
FROM surface
WHERE overall_priority NOT IN ('low', 'medium', 'high', 'critical')

UNION ALL

SELECT 'invalid_risk_values' AS check_name, COUNT(*) AS defect_count
FROM surface
WHERE fraud_risk NOT IN ('low', 'medium', 'high')
   OR aml_risk NOT IN ('low', 'medium', 'high')
   OR network_risk NOT IN ('low', 'medium', 'high')

UNION ALL

SELECT 'missing_queue_join_metrics' AS check_name, COUNT(*) AS defect_count
FROM surface
WHERE avg_queue_score IS NULL

UNION ALL

SELECT 'missing_case_overview' AS check_name, COUNT(*) AS defect_count
FROM surface
WHERE TRIM(COALESCE(case_overview, '')) = ''

UNION ALL

SELECT 'empty_recommended_actions' AS check_name, COUNT(*) AS defect_count
FROM surface
WHERE ARRAY_LENGTH(recommended_actions) IS NULL OR ARRAY_LENGTH(recommended_actions) = 0

UNION ALL

SELECT 'invalid_action_rank' AS check_name, COUNT(*) AS defect_count
FROM actions
WHERE action_rank < 1 OR TRIM(COALESCE(recommended_action, '')) = '';
