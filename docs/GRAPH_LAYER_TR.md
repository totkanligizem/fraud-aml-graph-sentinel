# Graph Layer Rehberi (TR)

Bu adimda local SQLite warehouse uzerinden graph odakli tablolar uretilir.

## Uretilen local tablolar

- `graph_party_node`
- `graph_party_edge`
- `graph_account_node`
- `graph_account_edge`
- `graph_party_cluster_membership`
- `graph_party_cluster_summary`

## 1) Local graph build

```bash
make graph-build
```

Varsayilan mantik:
- `fraud_scores` + `alert_queue` + `transaction_mart` birlestirilir
- party ve account node/edge tablolari uretilir
- supheli edge alt-grafindan party cluster'lari cikarilir

## 2) Local graph validation

```bash
make graph-validate
```

Bu kontrol sunlari dogrular:
- tablo varligi
- tablo bos olmamasi
- null/duplicate node id olmamasi
- risk score araligi (`0-1`)
- cluster membership / summary tutarliligi
- `party` ve `account` namespace'lerinin ayni literal kimlikleri paylasmamasi

## 3) Graph tablolarini BigQuery'ye yukle

```bash
make sqlite-graph-to-bq
```

BigQuery hedef tablolar:
- `dev_graph_party_node`
- `dev_graph_party_edge`
- `dev_graph_account_node`
- `dev_graph_account_edge`
- `dev_graph_party_cluster_membership`
- `dev_graph_party_cluster_summary`

## 4) BigQuery graph validation

```bash
make bq-validate-graph-state
```

## 5) BigQuery graph analytics tablolari

```bash
make bq-create-graph-analytics
```

Uretilen tablolar:
- `dev_graph_watchlist_party_top`
- `dev_graph_watchlist_cluster_top`

## 6) Tum graph BigQuery kontrolu

```bash
make bq-graph-check
```

## 7) Notlar

- Bu katman cekirdek fraud modelinin yerine gecmez; onu graph investigation lens ile tamamlar.
- `party` grafigi investigator ve AML narrative acisindan birinci onceliktir.
- `account` grafigi ise operasyonel drill-down icin faydalidir.
- Ayni ham kaynaktan turetilseler bile `party_id` ve `account_id` farkli namespace ile uretilir; downstream join/yorumlama hatalari boyle engellenir.
