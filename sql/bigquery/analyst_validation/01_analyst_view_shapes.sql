SELECT 'dev_analyst_case_summary' AS object_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_analyst_case_summary`

UNION ALL

SELECT 'dev_exec_analyst_surface' AS object_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_exec_analyst_surface`

UNION ALL

SELECT 'dev_exec_analyst_action_items' AS object_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_exec_analyst_action_items`;
