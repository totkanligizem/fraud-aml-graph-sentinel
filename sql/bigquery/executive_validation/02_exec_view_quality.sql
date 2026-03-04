WITH overview AS (
  SELECT * FROM `{{FULL_DATASET}}.dev_exec_overview_kpi`
),
dataset_surface AS (
  SELECT * FROM `{{FULL_DATASET}}.dev_exec_dataset_surface`
),
daily_surface AS (
  SELECT * FROM `{{FULL_DATASET}}.dev_exec_daily_surface`
),
queue_watchlist AS (
  SELECT * FROM `{{FULL_DATASET}}.dev_exec_queue_watchlist`
),
graph_watchlists AS (
  SELECT * FROM `{{FULL_DATASET}}.dev_exec_graph_watchlists`
)
SELECT
  'overview_single_row' AS check_name,
  COUNTIF(TRUE) AS observed_value
FROM overview

UNION ALL

SELECT
  'dataset_surface_unique_datasets' AS check_name,
  COUNT(DISTINCT dataset_id) AS observed_value
FROM dataset_surface

UNION ALL

SELECT
  'daily_surface_overview_rows' AS check_name,
  COUNT(*) AS observed_value
FROM daily_surface
WHERE lens_id = 'overview'

UNION ALL

SELECT
  'queue_watchlist_nonzero_rows' AS check_name,
  COUNT(*) AS observed_value
FROM queue_watchlist

UNION ALL

SELECT
  'graph_watchlists_nonzero_rows' AS check_name,
  COUNT(*) AS observed_value
FROM graph_watchlists

UNION ALL

SELECT
  'invalid_overview_scoring_coverage' AS check_name,
  COUNT(*) AS observed_value
FROM overview
WHERE scoring_coverage < 0 OR scoring_coverage > 1

UNION ALL

SELECT
  'invalid_dataset_share_of_volume' AS check_name,
  COUNT(*) AS observed_value
FROM dataset_surface
WHERE share_of_volume < 0 OR share_of_volume > 1

UNION ALL

SELECT
  'invalid_daily_top50_precision' AS check_name,
  COUNT(*) AS observed_value
FROM daily_surface
WHERE top50_precision < 0 OR top50_precision > 1

UNION ALL

SELECT
  'invalid_queue_rank' AS check_name,
  COUNT(*) AS observed_value
FROM queue_watchlist
WHERE dataset_rank < 1 OR overall_rank < 1

UNION ALL

SELECT
  'invalid_graph_watchlist_rank' AS check_name,
  COUNT(*) AS observed_value
FROM graph_watchlists
WHERE risk_rank < 1;
