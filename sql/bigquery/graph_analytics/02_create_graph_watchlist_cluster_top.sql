CREATE OR REPLACE TABLE `{{FULL_DATASET}}.dev_graph_watchlist_cluster_top` AS
SELECT
  cluster_id,
  party_count,
  edge_count,
  txn_count,
  amount_sum,
  fraud_event_count,
  aml_event_count,
  alert_event_count,
  high_risk_event_count,
  max_fraud_score,
  mean_edge_risk_score,
  max_edge_risk_score,
  first_seen,
  last_seen
FROM `{{FULL_DATASET}}.dev_graph_party_cluster_summary`
ORDER BY max_edge_risk_score DESC, party_count DESC, txn_count DESC
LIMIT 500;
