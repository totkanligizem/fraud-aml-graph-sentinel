# Vertex Analyst Copilot Rehberi (TR)

Bu faz, mevcut fraud/AML/graph pipeline'i Gemini tabanli analyst copilot ile tamamlamak icin hazirlik katmanidir.

## Secilen yol

- Platform: `Vertex AI`
- Varsayilan model: `gemini-2.5-flash`
- Escalation modeli: `gemini-2.5-pro`

Neden:
- BigQuery ve GCP altyapisi zaten hazir
- service account ve bucket kurulumu mevcut
- analyst copilot icin en dusuk operasyonel surtunme burada

## Hazirlanan input contract

1. `agent-casebook`
   - queue ve graph ciktilarindan deterministic case packet uretir

2. `agent-prompt-pack`
   - case packet'lardan provider-agnostic prompt dosyalari uretir

Bu iki katman sayesinde model baglantisi eklendiginde ham SQL veya rastgele prompt birlestirme yapmaya gerek kalmaz.

## Calistirma

```bash
make agent-casebook-validate
make agent-prompt-pack-validate
make agent-vertex-validate
```

Not:
- `agent-vertex-validate` smoke testi bilincli olarak `1` prompt ile calisir.
- Amac entegrasyon hattini dogrulamak; gereksiz kota tuketmek degil.
- Daha buyuk batch kosulari script uzerinden ayrica yapilir.

## Prompt mantigi

System prompt kurallari:
- sadece verilen packet uzerinden calis
- eksik veri uydurma
- observed fact / inference ayrimi yap
- fraud, AML ve graph sinyallerini karistirma
- audit-friendly kisa cikti ver

User payload:
- output contract
- queue-level case packet
- model routing bilgisi

## Sonraki gerçek entegrasyon adimi

Bir sonraki asamada eklenecekler:
1. Vertex AI SDK istemcisi
2. `vertex-aml-agent` identity ile auth
3. prompt pack -> Gemini request
4. JSON structured response parser
5. case summary artifact store

Bu adim artik kod tarafinda hazir:
- `run_vertex_analyst_copilot.py`
- `validate_vertex_analyst_outputs.py`

## Bu noktada senden ne gerekir?

Su an hicbir ek API key gerekmez.

Gercek model cagrisi fazina gectigimizde muhtemelen sadece:
- mevcut GCP/Vertex erisiminin kullanimi
- gerekirse `vertex-aml-agent` icin lokal test kimligi

OpenAI API bu asamada gerekli degil.
