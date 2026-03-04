# BigQuery Entegrasyon Rehberi (TR)

Bu dokuman, local SQLite ciktilarini BigQuery'ye tasimak icin gereken minimum adimlari verir.

## 1) Mevcut durum

Bu projede su dosyalar hazir:
- `scripts/setup_gcp_local.sh`
- `scripts/bigquery_test_connection.py`
- `scripts/sqlite_to_bigquery.py`
- `Makefile` hedefleri: `setup-gcp-local`, `bq-test`, `sqlite-to-bq-smoke`, `sqlite-to-bq-core`, `sqlite-to-bq-full`, `bq-full-check`, `bq-refresh-from-local-full`, `bq-create-executive-views`

## 2) IAM tarafinda zorunlu roller

Service account: `fraud-aml-graph@fraud-aml-graph.iam.gserviceaccount.com`

Asagidaki roller gerekli:
- Proje seviyesinde `roles/bigquery.jobUser` (query/load job baslatmak icin)
- Proje seviyesinde `roles/bigquery.user` (dataset create/check icin) veya
- Dataset onceden olusturulacaksa dataset seviyesinde `roles/bigquery.dataEditor`

Not:
- `bigquery.jobs.create` hatasi gorursen genelde `roles/bigquery.jobUser` eksiktir.

## 3) Lokal key/env kurulumu

```bash
make setup-gcp-local
```

Bu adim:
- key'i `.secrets/gcp-service-account.json` altina kopyalar
- `.env.local` uretir

## 4) Baglanti smoke test

```bash
make bq-test
```

Basarili olursa:
- `SELECT 1` query sonucu
- dataset hazir mesaji

## 5) SQLite -> BigQuery smoke upload

```bash
make sqlite-to-bq-smoke
```

Bu adim sinirli satir yukler (dev test):
- `dev_transaction_mart`
- `dev_feature_payer_24h`
- `dev_fraud_scores`
- `dev_alert_queue`

## 6) Core upload (daha buyuk)

```bash
make sqlite-to-bq-core
```

Tum local satirlari (capsiz) BigQuery'ye yenile:

```bash
make sqlite-to-bq-full
```

## 7) BigQuery analytics ve kalite kontrol

Analytics tablolarini uret:

```bash
make bq-create-analytics
```

State validator calistir (pass/fail):

```bash
make bq-validate-state
```

Validation SQL bundle calistir (CSV cikti uretir):

```bash
make bq-run-validation-sql
```

Tek komut tam kontrol:

```bash
make bq-full-check
```

Tek komut full refresh + tam kontrol:

```bash
make bq-refresh-from-local-full
```

Uretilen ciktilar:
- `artifacts/bigquery/validate-bigquery-state.json`
- `artifacts/bigquery/sql-runs/<run_id>/run-summary.json`
- `artifacts/bigquery/sql-runs/<run_id>/*.csv`

## 8) Executive view katmani

Bu katman, `dev_*` tablolari uzerinden canli yonetici/presentation view'lari uretir.

Calistirma:

```bash
make bq-create-executive-views
```

Uretilen BigQuery view'lari:
- `dev_exec_overview_kpi`
- `dev_exec_dataset_surface`
- `dev_exec_daily_surface`
- `dev_exec_queue_watchlist`
- `dev_exec_graph_watchlists`

Amac:
- dashboard / Looker / sunum katmani icin stabil semantic layer vermek
- local dashboard KPI mantigini BigQuery tarafinda da tekrar uretmek
- queue, graph ve daily risk yuzeyini tek yerden okumayi kolaylastirmak

## 9) Maliyet notu

- Load job'lar genelde query gibi tarama maliyeti yaratmaz.
- Asil maliyet kalemleri: depolama + sorgu tarama miktari.
- Smoke (50k satir/tablo) pratikte cok dusuk maliyetlidir.
- Full/coklu tekrar kosumlarda BigQuery faturalama panelinden izleme yap.

## 10) Guvenlik

- Key dosyasini asla repo'ya commit etme.
- Key yalnizca local `.secrets/` altinda kalsin.
- Key paylasildiysa IAM'dan key rotate et ve eski key'i revoke et.
