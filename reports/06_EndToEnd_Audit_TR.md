# End-to-End Audit Raporu (TR)

- Generated at (UTC): 2026-03-04T20:36:00Z
- Scope: Proje genelinde tam denetim (local + live BigQuery + live Vertex)
- Sonuc: PASS

## 1) Yerel Kalite Kapilari

- `python3 -m compileall -q scripts`: PASS
- `make check-datasets`: PASS
- `python3 scripts/cleanup_incomplete_runs.py`: PASS (`candidate_count=0`)
- `make validate-state`: PASS
- `make graph-validate`: PASS
- `make dashboard-check`: PASS
- `make report-briefing`: PASS
- `make validate-executive-sql`: PASS
- `make validate-analyst-sql`: PASS
- `make agent-casebook-validate`: PASS
- `make agent-prompt-pack-validate`: PASS
- SQLite:
  - `PRAGMA integrity_check = ok`
  - `PRAGMA quick_check = ok`

## 2) Canli Vertex Denetimi

- `make agent-vertex-batch-validate`: PASS
- Latest run:
  - `run_id=20260304T202949Z`
  - `response_count=3`
  - `error_count=0`
  - `promoted_to_latest=true`
- Not:
  - `ieee_cis` ve `paysim` case'lerinde deterministic fallback aktif, ancak output contract tam uyumlu.

## 3) Canli BigQuery Denetimi

- `make bq-full-check`: PASS
- `make bq-graph-check`: PASS
- `make bq-validate-executive-views`: PASS
- `make bq-analyst-check`: PASS

### Executive View Durumu

- `dev_exec_overview_kpi`: `1`
- `dev_exec_dataset_surface`: `4`
- `dev_exec_daily_surface`: `180`
- `dev_exec_queue_watchlist`: `88`
- `dev_exec_graph_watchlists`: `717,354`

Kalite kontrolleri:

- `invalid_overview_scoring_coverage = 0`
- `invalid_dataset_share_of_volume = 0`
- `invalid_daily_top50_precision = 0`
- `invalid_queue_rank = 0`
- `invalid_graph_watchlist_rank = 0`

### Analyst View Durumu

- `dev_analyst_case_summary`: `3`
- `dev_exec_analyst_surface`: `3`
- `dev_exec_analyst_action_items`: `12`

Kalite kontrolleri:

- `empty_recommended_actions = 0`
- `missing_queue_join_metrics = 0`
- `invalid_action_rank = 0`
- `invalid_risk_values = 0`
- `missing_case_overview = 0`
- `invalid_overall_priority = 0`

## 4) Dashboard Publish Durumu

- `artifacts/dashboard/validate-dashboard-state.json`: `ok=true`
- KPI/quality summary:
  - `dataset_count=4`
  - `total_transactions=1,184,807`
  - `total_scored_rows=884,807`
  - `passed_checks=13`
  - `total_checks=13`
  - `total_defects=0`
- DOM binding:
  - `html_id_count=32`
  - `js_bound_id_count=32`
  - `missing_html_ids=[]`
  - `duplicate_html_ids=[]`

## 5) Bu Turda Revize Gereksinimi

- Yeni bug veya bloklayan hata bulunmadi.
- Ek revize uygulanmadi; mevcut kod ve veri akisi denetimden temiz gecti.

## 6) Nihai Karar

- Proje su anki kapsamla publish icin hazir.
- Audit kapanisi: **READY FOR PUBLISH**.
