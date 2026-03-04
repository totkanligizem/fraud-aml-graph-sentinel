DROP TABLE IF EXISTS transaction_mart;

CREATE TABLE transaction_mart AS
SELECT
  *,
  date(event_time) AS event_date,
  strftime('%Y-%m-%dT%H:00:00', event_time) AS event_hour
FROM stg_transaction_event;

CREATE INDEX IF NOT EXISTS idx_tm_event_time ON transaction_mart(event_time);
CREATE INDEX IF NOT EXISTS idx_tm_payer_event_time ON transaction_mart(payer_party_id, event_time);
CREATE INDEX IF NOT EXISTS idx_tm_dataset ON transaction_mart(dataset_id);
CREATE INDEX IF NOT EXISTS idx_tm_event_id ON transaction_mart(event_id);
