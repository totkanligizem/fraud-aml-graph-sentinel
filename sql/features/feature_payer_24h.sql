-- Replace {{feature_base_limit}} with an integer before running.
DROP TABLE IF EXISTS feature_payer_24h;

CREATE TABLE feature_payer_24h AS
WITH base AS (
  SELECT
    event_id,
    event_time,
    payer_party_id
  FROM transaction_mart
  WHERE payer_party_id IS NOT NULL
  ORDER BY event_time
  LIMIT {{feature_base_limit}}
)
SELECT
  b.event_id,
  b.event_time,
  b.payer_party_id,
  (
    SELECT COUNT(*)
    FROM transaction_mart h
    WHERE h.payer_party_id = b.payer_party_id
      AND h.event_time < b.event_time
      AND h.event_time >= datetime(b.event_time, '-24 hours')
  ) AS payer_txn_count_24h,
  COALESCE((
    SELECT SUM(h.amount)
    FROM transaction_mart h
    WHERE h.payer_party_id = b.payer_party_id
      AND h.event_time < b.event_time
      AND h.event_time >= datetime(b.event_time, '-24 hours')
  ), 0.0) AS payer_amt_sum_24h
FROM base b;
