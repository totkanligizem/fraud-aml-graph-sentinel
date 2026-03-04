# Dashboard Content Freeze Checklist (TR)

- Generated at (UTC): 2026-03-04T20:22:27Z
- Scope: Publish polish sonrasi son kontrol turu
- Result: PASS

## 1) Build ve Validator Kapilari

- `make dashboard-check`: PASS
- `make report-briefing`: PASS
- `python3 -m compileall -q scripts`: PASS

## 2) Dashboard Bundle Quality

- `artifacts/dashboard/validate-dashboard-state.json`: `ok=true`
- `payload_summary.total_transactions`: `1,184,807`
- `payload_summary.total_scored_rows`: `884,807`
- `payload_summary.passed_checks / total_checks`: `13 / 13`
- `payload_summary.total_defects`: `0`
- DOM binding: `missing_html_ids=[]`, `duplicate_html_ids=[]`

## 3) KPI ve Artifact Tutarlilik Kontrolu

- `kpi_total_transactions == snapshot.transaction_mart`: PASS
- `kpi_scored_rows == snapshot.total_scored_rows`: PASS
- `kpi_queue_count == ranking.queue_count`: PASS
- `quality.total_defects == 0`: PASS
- `analyst.response_count == vertex.response_count`: PASS
- `analyst.error_count == vertex.error_count`: PASS

## 4) Canli Kaynak Referanslari

- Vertex latest run: `artifacts/agent/vertex_responses/latest/run-summary.json`
  - `run_id=20260304T195616Z`
  - `response_count=3`
  - `error_count=0`
  - `promoted_to_latest=true`
- Executive validation CSV:
  - `artifacts/bigquery/sql-runs/20260304T201103Z/01_exec_view_shapes.csv`
  - `artifacts/bigquery/sql-runs/20260304T201103Z/02_exec_view_quality.csv`
- Analyst validation CSV:
  - `artifacts/bigquery/sql-runs/20260304T201110Z/01_analyst_view_shapes.csv`
  - `artifacts/bigquery/sql-runs/20260304T201110Z/02_analyst_view_quality.csv`

## 5) Release Karari

- Dashboard publish icin bloklayan defect yok.
- Content freeze checklist kapanisi: **READY FOR PUBLISH**.
