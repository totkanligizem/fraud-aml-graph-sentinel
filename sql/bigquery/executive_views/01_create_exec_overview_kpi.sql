CREATE OR REPLACE VIEW `{{FULL_DATASET}}.dev_exec_overview_kpi` AS
WITH tm AS (
  SELECT
    COUNT(*) AS transaction_rows,
    SUM(COALESCE(label_fraud, 0)) AS fraud_rows,
    SUM(COALESCE(label_aml, 0)) AS aml_rows,
    AVG(amount) AS avg_amount,
    MIN(event_date) AS first_event_date,
    MAX(event_date) AS last_event_date
  FROM `{{FULL_DATASET}}.dev_transaction_mart`
),
fs AS (
  SELECT
    COUNT(*) AS scored_rows,
    AVG(fraud_score) AS avg_fraud_score,
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(95)] AS p95_fraud_score,
    SUM(CASE WHEN fraud_score >= 0.8 THEN 1 ELSE 0 END) AS high_risk_scored_rows
  FROM `{{FULL_DATASET}}.dev_fraud_scores`
),
aq AS (
  SELECT
    COUNT(*) AS queue_rows,
    COUNT(DISTINCT queue_id) AS queue_count,
    AVG(fraud_score) AS avg_queue_score,
    APPROX_QUANTILES(fraud_score, 100)[OFFSET(95)] AS p95_queue_score
  FROM `{{FULL_DATASET}}.dev_alert_queue`
),
top50 AS (
  SELECT
    COUNT(*) AS top50_rows,
    SUM(COALESCE(label_fraud, 0)) AS top50_true_fraud_rows,
    SAFE_DIVIDE(SUM(COALESCE(label_fraud, 0)), NULLIF(COUNT(*), 0)) AS top50_precision_micro
  FROM `{{FULL_DATASET}}.dev_alert_queue`
  WHERE rank_in_queue <= 50
),
queue_precision AS (
  SELECT AVG(precision_at_50) AS mean_precision_at_50
  FROM (
    SELECT
      queue_id,
      SAFE_DIVIDE(SUM(COALESCE(label_fraud, 0)), NULLIF(COUNT(*), 0)) AS precision_at_50
    FROM `{{FULL_DATASET}}.dev_alert_queue`
    WHERE rank_in_queue <= 50
    GROUP BY queue_id
  )
),
graph_clusters AS (
  SELECT
    COUNT(*) AS graph_cluster_count,
    SUM(CASE WHEN max_edge_risk_score >= 0.65 OR high_risk_event_count > 0 THEN 1 ELSE 0 END) AS suspicious_cluster_count,
    SUM(party_count) AS graph_cluster_party_count,
    SUM(edge_count) AS graph_cluster_edge_count,
    MAX(max_edge_risk_score) AS max_cluster_risk_score
  FROM `{{FULL_DATASET}}.dev_graph_party_cluster_summary`
),
graph_edges AS (
  SELECT
    COUNT(*) AS graph_party_edge_count,
    SUM(CASE WHEN risk_score >= 0.65 THEN 1 ELSE 0 END) AS suspicious_party_edge_count,
    AVG(risk_score) AS avg_party_edge_risk_score
  FROM `{{FULL_DATASET}}.dev_graph_party_edge`
),
graph_nodes AS (
  SELECT COUNT(*) AS graph_party_node_count
  FROM `{{FULL_DATASET}}.dev_graph_party_node`
)
SELECT
  CURRENT_TIMESTAMP() AS view_generated_at,
  tm.transaction_rows,
  fs.scored_rows,
  aq.queue_rows,
  aq.queue_count,
  top50.top50_rows,
  top50.top50_true_fraud_rows,
  tm.fraud_rows,
  tm.aml_rows,
  tm.avg_amount,
  tm.first_event_date,
  tm.last_event_date,
  fs.avg_fraud_score,
  fs.p95_fraud_score,
  fs.high_risk_scored_rows,
  aq.avg_queue_score,
  aq.p95_queue_score,
  top50.top50_precision_micro,
  queue_precision.mean_precision_at_50,
  graph_nodes.graph_party_node_count,
  graph_edges.graph_party_edge_count,
  graph_edges.suspicious_party_edge_count,
  graph_edges.avg_party_edge_risk_score,
  graph_clusters.graph_cluster_count,
  graph_clusters.suspicious_cluster_count,
  graph_clusters.graph_cluster_party_count,
  graph_clusters.graph_cluster_edge_count,
  graph_clusters.max_cluster_risk_score,
  SAFE_DIVIDE(fs.scored_rows, NULLIF(tm.transaction_rows, 0)) AS scoring_coverage,
  SAFE_DIVIDE(tm.fraud_rows, NULLIF(tm.transaction_rows, 0)) AS fraud_rate,
  SAFE_DIVIDE(tm.aml_rows, NULLIF(tm.transaction_rows, 0)) AS aml_rate,
  SAFE_DIVIDE(fs.high_risk_scored_rows, NULLIF(fs.scored_rows, 0)) AS high_risk_scored_share,
  SAFE_DIVIDE(graph_edges.suspicious_party_edge_count, NULLIF(graph_edges.graph_party_edge_count, 0)) AS suspicious_party_edge_ratio
FROM tm
CROSS JOIN fs
CROSS JOIN aq
CROSS JOIN top50
CROSS JOIN queue_precision
CROSS JOIN graph_nodes
CROSS JOIN graph_edges
CROSS JOIN graph_clusters;
