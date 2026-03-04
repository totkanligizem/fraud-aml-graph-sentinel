# Ingestion Baslangic Rehberi

Bu adimda 4 zorunlu datasetten kanonik `transaction_event` ciktilari uretiriz.

## Script
- `scripts/ingest_canonical.py`

## 1) Smoke test (hizli deneme)

```bash
python3 scripts/ingest_canonical.py --dataset all --max-rows 1000 --chunksize 50000
```

Bu komut her datasetten en fazla 1000 satir isler ve pipeline'in calistigini dogrular.

## 2) Tam ingestion (tum satirlar)

```bash
python3 scripts/ingest_canonical.py --dataset all --chunksize 200000
```

Not:
- IBM AML-Data dosyalari cok buyuk, bu adim uzun surebilir.

## 3) Cikti konumu

Varsayilan output:
- `data/curated/transaction_event/<dataset_id>/<run_id>/part-xxxxx.csv`
- `data/curated/transaction_event/<dataset_id>/<run_id>/manifest.json`
- `data/curated/transaction_event/run-summary-<run_id>.json`

## 4) Dataset bazli calistirma

Ornek:

```bash
python3 scripts/ingest_canonical.py --dataset paysim --max-rows 200000
```

Desteklenen dataset argumanlari:
- `ieee_cis`
- `creditcard_fraud`
- `paysim`
- `ibm_aml_data`
- `all`
