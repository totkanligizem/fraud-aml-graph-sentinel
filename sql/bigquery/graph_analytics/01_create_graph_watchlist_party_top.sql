CREATE OR REPLACE TABLE `{{FULL_DATASET}}.dev_graph_watchlist_party_top` AS
SELECT
  party_id,
  first_seen,
  last_seen,
  total_txn_count,
  total_amount_sum,
  distinct_counterparty_count,
  fraud_event_count,
  aml_event_count,
  alert_event_count,
  high_risk_event_count,
  max_fraud_score,
  risk_score
FROM `{{FULL_DATASET}}.dev_graph_party_node`
ORDER BY risk_score DESC, max_fraud_score DESC, total_amount_sum DESC
LIMIT 1000;
