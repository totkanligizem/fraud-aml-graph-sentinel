CREATE OR REPLACE VIEW `{{FULL_DATASET}}.dev_exec_graph_watchlists` AS
SELECT
  'party' AS watchlist_type,
  party_id AS entity_id,
  CAST(NULL AS STRING) AS cluster_id,
  total_txn_count AS txn_count,
  distinct_counterparty_count AS breadth_count,
  total_amount_sum AS amount_sum,
  high_risk_event_count,
  risk_score,
  DENSE_RANK() OVER (
    ORDER BY risk_score DESC, total_amount_sum DESC, total_txn_count DESC, party_id ASC
  ) AS risk_rank
FROM `{{FULL_DATASET}}.dev_graph_party_node`

UNION ALL

SELECT
  'cluster' AS watchlist_type,
  CAST(NULL AS STRING) AS entity_id,
  cluster_id,
  txn_count,
  party_count AS breadth_count,
  amount_sum,
  high_risk_event_count,
  max_edge_risk_score AS risk_score,
  DENSE_RANK() OVER (
    ORDER BY max_edge_risk_score DESC, amount_sum DESC, txn_count DESC, cluster_id ASC
  ) AS risk_rank
FROM `{{FULL_DATASET}}.dev_graph_party_cluster_summary`;
