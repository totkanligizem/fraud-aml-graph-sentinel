SELECT 'dev_exec_overview_kpi' AS view_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_exec_overview_kpi`

UNION ALL

SELECT 'dev_exec_dataset_surface' AS view_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_exec_dataset_surface`

UNION ALL

SELECT 'dev_exec_daily_surface' AS view_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_exec_daily_surface`

UNION ALL

SELECT 'dev_exec_queue_watchlist' AS view_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_exec_queue_watchlist`

UNION ALL

SELECT 'dev_exec_graph_watchlists' AS view_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_exec_graph_watchlists`;
