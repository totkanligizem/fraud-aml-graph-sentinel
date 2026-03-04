DROP TABLE IF EXISTS stg_transaction_event;

CREATE TABLE stg_transaction_event AS
SELECT
  event_id,
  CAST(source_event_id AS TEXT) AS source_event_id,
  dataset_id,
  event_time,
  event_time_grain,
  time_step,
  payer_party_id,
  payee_party_id,
  payer_account_id,
  payee_account_id,
  COALESCE(NULLIF(channel, ''), 'UNKNOWN') AS channel,
  COALESCE(NULLIF(txn_type, ''), 'UNKNOWN') AS txn_type,
  mcc_category,
  CAST(amount AS REAL) AS amount,
  COALESCE(NULLIF(currency, ''), 'USD') AS currency,
  CAST(fx_rate_to_usd AS REAL) AS fx_rate_to_usd,
  device_id,
  ip_prefix,
  email_domain,
  geo,
  CAST(label_fraud AS INTEGER) AS label_fraud,
  CAST(label_aml AS INTEGER) AS label_aml,
  label_source,
  pii_class,
  consent_class,
  retention_class,
  raw_partition,
  ingested_at,
  adapter_version
FROM transaction_event_raw
WHERE event_time IS NOT NULL;
