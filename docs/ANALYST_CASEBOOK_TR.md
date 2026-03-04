# Analyst Casebook Rehberi (TR)

Bu katman, investigation queue ve graph ciktilarindan LLM'e hazir deterministic case packet'lar uretir.

Amaç:
- Gemini / Vertex analyst copilot fazina gecmeden once temiz input contract kurmak
- Prompt tarafinda ham SQL veya dağinik join mantigi tasimamak
- Her queue icin tekrar uretilebilir, denetlenebilir ve versiyonlu case paketleri saglamak

## Uretilen artefaktlar

- `artifacts/agent/casebook/<run_id>/casebook.json`
- `artifacts/agent/casebook/<run_id>/casebook.md`
- `artifacts/agent/casebook/latest/casebook.json`
- `artifacts/agent/casebook/latest/casebook.md`

## Calistirma

```bash
make agent-casebook
make agent-casebook-validate
```

## Icerik

Her packet su bloklari icerir:
- queue kimligi ve dataset
- queue metrikleri
- top ranked event listesi
- packet icindeki party/account yuzeyi
- graph party watchlist
- graph cluster watchlist
- deterministic evidence note'lari

Secim mantigi:
- once her scored dataset icin en az 1 queue seed edilir
- sonra `per_dataset_cap` dahilinde doldurulur
- gerekirse global rank ile kalan slotlar tamamlanir

## Neden kritik?

- LLM provider degisse bile input contract stabil kalir
- Analyst summary / case note / escalation memo gibi ciktılar icin tek kaynak olur
- Model entegrasyonundan once veri ve prompt disiplini saglar

## Sonraki faz

Bu katman hazirken:
1. Vertex AI + Gemini ile case summary generation
2. queue-level explanation prompt'lari
3. graph cluster narrative
4. investigator action recommendation
