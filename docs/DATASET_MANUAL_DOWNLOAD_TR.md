# Dataset Manual Download Rehberi

Bu projede ilk faz icin **zorunlu** datasetler ve klasor yerlesimi asagidadir.

## 1) Zorunlu datasetler (ilk gelistirme fazi)

### A. IEEE-CIS Fraud Detection (Kaggle competition)
- Link: https://www.kaggle.com/competitions/ieee-fraud-detection
- Hedef klasor:
  - `data/raw/ieee_cis/original/`
- Not:
  - Kaggle hesabiyla manuel indir.
  - Inen zip'i bu klasore koy.
  - Cikartilan CSV dosyalari `data/raw/ieee_cis/extracted/` altina acilacak.

### B. Credit Card Fraud Detection (Kaggle ULB)
- Link: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud
- Hedef klasor:
  - `data/raw/creditcard_fraud/original/`
- Not:
  - `creditcard.csv` dosyasi cikinca `data/raw/creditcard_fraud/extracted/` altina koy.

### C. PaySim1 (Kaggle)
- Link: https://www.kaggle.com/datasets/ealaxi/paysim1
- Hedef klasor:
  - `data/raw/paysim/original/`
- Not:
  - Cikartilan ana dosyayi (`PS_20174392719_1491204439457_log.csv`) `data/raw/paysim/extracted/` altina koy.

### D. IBM AML-Data (GitHub)
- Link: https://github.com/IBM/AML-Data
- Hedef klasor:
  - `data/raw/ibm_aml_data/original/`
- Not:
  - Repo zip indirip bu klasore koy.
  - Islenecek CSV dosyalarini `data/raw/ibm_aml_data/extracted/` altina kopyala.

## 2) Ikinci faz datasetler (opsiyonel ama onerilir)

### E. BankSim1 (Kaggle)
- Link: https://www.kaggle.com/datasets/ealaxi/banksim1
- Hedef klasor:
  - `data/raw/banksim/original/`
  - `data/raw/banksim/extracted/`

### F. IBM AMLSim (GitHub)
- Link: https://github.com/IBM/AMLSim
- Hedef klasor:
  - `data/raw/ibm_amlsim/original/`
  - `data/raw/ibm_amlsim/extracted/`

### G. Elliptic Data Set (Kaggle)
- Link: https://www.kaggle.com/datasets/ellipticco/elliptic-data-set
- Hedef klasor:
  - `data/raw/elliptic/original/`
  - `data/raw/elliptic/extracted/`

## 3) Hizli kontrol listesi

- Her dataset once `original/` altina zip/repo olarak gelsin.
- Islenecek CSV/parquet dosyalari `extracted/` altina acilsin.
- Ham datasetleri asla git'e commit etme.

## 4) Onerilen ilk indirme sirasi

1. `ieee_cis`
2. `creditcard_fraud`
3. `paysim`
4. `ibm_aml_data`

Bu 4'u tamamlayinca ingestion + canonical schema gelistirmesine baslayacagiz.
