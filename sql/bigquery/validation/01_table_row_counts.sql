SELECT 'dev_transaction_mart' AS table_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_transaction_mart`
UNION ALL
SELECT 'dev_feature_payer_24h' AS table_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_feature_payer_24h`
UNION ALL
SELECT 'dev_monitoring_mart' AS table_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_monitoring_mart`
UNION ALL
SELECT 'dev_fraud_scores' AS table_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_fraud_scores`
UNION ALL
SELECT 'dev_alert_queue' AS table_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_alert_queue`
UNION ALL
SELECT 'dev_alert_queue_top50' AS table_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_alert_queue_top50`
UNION ALL
SELECT 'dev_risk_dashboard_daily' AS table_name, COUNT(*) AS row_count
FROM `{{FULL_DATASET}}.dev_risk_dashboard_daily`;
