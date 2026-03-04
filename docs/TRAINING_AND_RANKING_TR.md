# Training ve Ranking Rehberi

Bu adimda:
1. Fraud baseline modeli egitilir
2. Model skor uretir
3. Soruşturma kuyruğu (ranking) metrikleri hesaplanir

## 1) Fraud baseline smoke training

```bash
make train-fraud-smoke
```

Cikti:
- `artifacts/models/fraud_baseline/<run_id>/model.npz`
- `artifacts/models/fraud_baseline/<run_id>/metrics.json`
- `artifacts/models/fraud_baseline/latest` (son run'a symlink)

## 2) Fraud scoring (smoke)

```bash
python3 scripts/score_fraud_baseline_numpy.py \
  --model-path artifacts/models/fraud_baseline/latest/model.npz \
  --max-rows-per-dataset 50000 \
  --chunksize 50000
```

SQLite tablo:
- `fraud_scores`

## 3) Investigation queue + ranking metrikleri

```bash
python3 scripts/build_investigation_queue.py --top-k 50
```

SQLite tablo:
- `alert_queue`

Artifact:
- `artifacts/models/ranking/<run_id>/ranking-summary.json`
- `artifacts/models/ranking/<run_id>/queue_metrics.csv`
