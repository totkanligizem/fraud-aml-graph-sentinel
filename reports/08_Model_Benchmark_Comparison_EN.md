# Model Benchmark Comparison (EN)

Generated at (UTC): 2026-03-05T16:43:59Z

## Baseline vs Benchmark

| Metric | Baseline | Benchmark Raw | Benchmark Calibrated | Delta (Calibrated - Baseline) |
|---|---:|---:|---:|---:|
| Average Precision | 0.071020 | 0.072538 | 0.072538 | 0.001518 |
| PR-AUC (trapz) | 0.070823 | 0.072454 | 0.072454 | 0.001631 |
| ROC-AUC | n/a | 0.872449 | 0.872449 | n/a |

## Queue Ranking Comparison

| Metric | Baseline | Benchmark | Delta |
|---|---:|---:|---:|
| Mean Precision@50 | 0.069091 | 0.087273 | 0.018182 |
| Mean NDCG@50 | 0.069647 | 0.081983 | 0.012336 |

## Optional Tree Benchmark

| Metric | Baseline | Tree Calibrated | Delta |
|---|---:|---:|---:|
| Average Precision | 0.071020 | 0.138502 | 0.067482 |
| PR-AUC (trapz) | 0.070823 | 0.137825 | 0.067002 |
| ROC-AUC | n/a | 0.904592 | n/a |
| Mean Precision@50 | 0.069091 | 0.270682 | 0.201591 |
| Mean NDCG@50 | 0.069647 | 0.298313 | 0.228666 |

## Subtask Evaluation (label_type)

| Layer | label_type | Rows | Positive rate | AP | PR-AUC |
|---|---|---:|---:|---:|---:|
| Baseline | fraud | 884807 | 0.012174 | 0.057341 | 0.057309 |
| Benchmark | fraud | 884807 | 0.012174 | 0.064728 | 0.064593 |
| Tree | fraud | 884807 | 0.012174 | 0.156150 | 0.155972 |

## Notes

- Benchmark model: interaction-augmented numpy logistic regression with Platt calibration.
- Artifact selection is pinned to db_path: `data/warehouse/ledger_sentinel.db`.
- Ranking summaries are loaded from db-matched baseline/benchmark artifacts.
