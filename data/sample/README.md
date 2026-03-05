# Synthetic Sample Data

This folder is used for reproducible local and CI smoke runs.

The sample data is generated at runtime by:

```bash
make sample-generate
```

It produces canonical-format CSV and `manifest.json` files under:

- `data/sample/transaction_event/<dataset_id>/<run_id>/part-00001.csv`

Run full sample pipeline:

```bash
make sample-e2e
```

The sample pipeline builds:

- `data/sample/warehouse/ledger_sentinel_sample.db`
- baseline model artifacts in `artifacts/models/fraud_baseline/latest/`
- sample ranking artifacts in `artifacts/models/ranking_sample/latest/`

Notes:

- Data is synthetic and deterministic (seeded), not production data.
- This path is intended for reproducibility and CI checks only.
