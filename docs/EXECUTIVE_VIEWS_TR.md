# BigQuery Executive Views Rehberi (TR)

Bu katman, mevcut `dev_*` BigQuery tablolarindan canli ve paylasima uygun semantic view'lar uretir.

Amaç:
- dashboard ile BigQuery tarafindaki KPI mantigini hizalamak
- Looker Studio / paylasim katmani icin tekrar kullanilabilir bir presentation layer vermek
- transaction, scoring, queue ve graph sinyallerini tek bir ust katmanda toplamak

## Calistirma

```bash
make bq-create-executive-views
```

Validation:

```bash
make bq-validate-executive-views
```

SQL klasoru:

```text
sql/bigquery/executive_views/
```

## Uretilen view'lar

### 1) `dev_exec_overview_kpi`

Tek satirlik executive ozet.

Icerik:
- toplam transaction
- toplam scored row
- toplam queue row
- queue sayisi
- top50 precision
- fraud / AML rate
- high-risk score share
- graph cluster ve edge sinyalleri

Kullanim:
- hero KPI
- executive summary kartlari
- haftalik status toplantisi girisi

### 2) `dev_exec_dataset_surface`

Dataset bazli risk lens.

Icerik:
- dataset volume
- fraud / AML row sayilari
- avg amount
- scoring coverage
- avg / max queue score
- top50 precision
- share of volume

Kullanim:
- dataset karsilastirma tablosu
- hangi veri kaynagi daha riskli sorusu
- publish dashboard dataset matrix

### 3) `dev_exec_daily_surface`

Gunluk risk ritmi.

Icerik:
- `overview` lens + dataset lens'leri
- txn / fraud / AML sayilari
- scored row
- avg / p95 fraud score
- top50 alert count
- top50 precision
- high-risk scored share

Kullanim:
- trend chart
- daily ops board
- management risk pulse

### 4) `dev_exec_queue_watchlist`

Queue bazli investigation leaderboard.

Icerik:
- dataset, queue_id, event_date
- queue row count
- avg / max queue score
- positive row count
- top50 precision
- dataset rank ve overall rank

Kullanim:
- hangi queue once bakilmali
- operasyon ekibi icin daily shortlist

### 5) `dev_exec_graph_watchlists`

Graph tarafini tek view icinde toplar.

Tipler:
- `party`
- `cluster`

Icerik:
- entity / cluster id
- txn count
- breadth count
- amount sum
- high-risk event count
- risk score
- risk rank

Kullanim:
- graph narrative
- supheli actor / cluster watchlist

## Tasarim ilkeleri

- `TABLE` yerine `VIEW` kullanilir; local -> BigQuery refresh sonrasi otomatik guncel kalir
- mevcut `dev_*` tablo isimleri korunur; semantic ust katman yalnizca read path ekler
- fraud ve AML ayni view'da olsa da farkli metric kolonlari ile ayrik tutulur
- no-score kaynaklar icin fraud scoring kolonlari `0` / `NULL-safe` mantikla doner; sahte fallback uretilmez

## Kritik notlar

- Bu view'lar local snapshot raporunun yerine gecmez; analytical consumption katmanidir
- BigQuery tarafinda canli yeniden dogrulama yapilabiliyorsa `bq-test` sonrasi kosulmalidir
- Looker veya baska BI araci baglanacaksa ilk tercih bu `dev_exec_*` view'lari olmali; ham `dev_*` tablolari degil

## Onerilen kullanim sirasi

1. `make bq-test`
2. `make bq-create-analytics`
3. `make bq-create-graph-analytics`
4. `make bq-create-executive-views`
5. `make bq-validate-executive-views`
6. Sonra Looker / dashboard / sharing katmani
