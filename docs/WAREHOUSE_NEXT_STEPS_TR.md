# Local Warehouse (SQLite) Rehberi

Bu adimda kanonik CSV ciktilari local SQLite veritabanina yuklenir ve
staging/mart/feature tablolari olusturulur.

## Script
- `scripts/build_sqlite_warehouse.py`

## 1) Guvenli baslangic (RAM/SSD dostu)

Asagidaki komut, her datasetten en fazla 300 bin satirla calisir:

```bash
python3 scripts/build_sqlite_warehouse.py \
  --datasets ieee_cis creditcard_fraud paysim ibm_aml_data \
  --max-rows-per-dataset 300000 \
  --chunksize 50000 \
  --feature-base-limit 150000 \
  --recreate
```

## 2) Tam yukleme (daha agir)

```bash
python3 scripts/build_sqlite_warehouse.py \
  --datasets ieee_cis creditcard_fraud paysim ibm_aml_data \
  --chunksize 100000 \
  --feature-base-limit 200000 \
  --recreate
```

Not:
- Tam yukleme disk ve sure olarak maliyetlidir.
- `feature_base_limit` buyudukce leakage-safe feature tablosunun hesap suresi artar.

## 3) Ciktilar

- SQLite DB: `data/warehouse/ledger_sentinel.db`
- Build ozeti: `data/warehouse/warehouse-build-summary.json`

## 4) Uretilen tablolar

- `transaction_event_raw`
- `stg_transaction_event`
- `transaction_mart`
- `feature_payer_24h`
- `monitoring_mart`
