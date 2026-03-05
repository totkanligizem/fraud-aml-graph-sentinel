# Vertex AI / Agent Yol Haritasi (TR)

Bu dokuman, mevcut Fraud - AML Graph projesi icin Vertex AI, Gemini ve Agent Engine
tarafinda hangi adimlarin hangi sirayla alinmasi gerektigini netlestirir.

Temel ilke:
- Simdi sadece gerekli altyapiyi hazirla.
- Gereksiz servis, endpoint, model deployment veya agent olusturma.
- LLM/agent katmanini sadece analyst copilot ve raporlama icin ekle.

## 1) Simdi ne yapmali?

Simdi alinmasi mantikli ve dusuk riskli adimlar:
- Vertex AI API'nin acik oldugunu dogrula.
- Vertex tarafinda tek bir standart calisma bolgesi sec.
- Agent icin ayri bir service account hazirla.
- Ileride agent deployment/artifact icin bir Cloud Storage bucket hazirla.
- Gemini'yi test etmek istersen Vertex AI Studio kullan, ama production auth icin API key'e baglanma.

## 2) Simdi ne yapmamali?

Su asamada sunlari olusturma:
- Model Registry kaydi
- Vertex Endpoint
- Batch Inference job
- Agent Designer icinde kalici agent
- Agent Engine deployment
- OpenAI API key

Gerekce:
- Bizim cekirdek fraud/AML pipeline zaten local + BigQuery tarafinda calisiyor.
- Henuz agent davranisi, tool seti ve case workflow kontrati finalize edilmedi.
- Erken agent kurmak yalnizca maliyet, IAM karmasasi ve region karmasasi yaratir.

## 3) Standart region karari

Onerilen standart:
- BigQuery dataset: `EU` (zaten hazir)
- Vertex AI / Gemini / Agent tarafi tek-bolge: `europe-west4`

Neden:
- Vertex AI Agent Engine bu bolgeyi destekler.
- Gemini Vertex endpoint'leri Avrupa tarafinda bu bolgede mevcut.
- BigQuery `EU` multi-region ile ayni cografi alanda kalir.

Not:
- `europe-central2 (Warsaw)` Vertex dashboard/Studio icin gorunebilir.
- Ancak Agent Engine tarafinda ana standart bolge olarak `europe-west4` secmek daha guvenlidir.

## 4) Konsolda adim adim ne olusturulacak?

### Adim 1: Vertex region'i sabitle

Konsolda:
- Vertex AI Dashboard'a gir
- Region secicisinden `europe-west4 (Netherlands)` sec

Hedef:
- Bundan sonra Vertex Studio, Agent Designer ve Agent Engine ekranlarina mumkun oldugunca ayni region ile bak.

### Adim 2: Service account olustur

Olusturulacak hesap:
- `vertex-aml-agent`

Beklenen email formati:
- `vertex-aml-agent@YOUR_GCP_PROJECT_ID.iam.gserviceaccount.com`

Aciklama:
- `Fraud AML analyst copilot runtime identity`

### Adim 3: Service account rollerini ver

Bu service account'a verilecek minimum roller:
- `Vertex AI User`
- `BigQuery Data Viewer`
- `BigQuery Job User`

Eger agent sonradan BigQuery'ye sonuc yazacaksa ekle:
- `BigQuery Data Editor`

Eger agent Cloud Storage artifact/bellek okuyacaksa ekle:
- `Storage Object Admin`

## 5) Cloud Storage bucket hazirla

Olustur:
- Bucket name (ornek): `your-project-vertex-ew4`
- Region: `europe-west4`
- Access control: Uniform
- Public access: kapali

Kullanim amaci:
- ileride agent package/artifact
- prompt/version export
- opsiyonel report export

## 6) API key gerekli mi?

Simdi: hayir, zorunlu degil.

En dogru kullanim:
- Test icin: istersen Vertex API key alinabilir
- Production/backend icin: mevcut service account / ADC kullan

Bu proje icin tercih:
- Backend ve agent entegrasyonu gelirse API key yerine service account kullan
- API key'i sadece Vertex AI Studio veya hizli manuel test icin dusun

## 7) Gemini tarafinda ne yapacagiz?

Bu projede Gemini'yi cekirdek fraud skorlamada kullanmayacagiz.

Gemini'nin kullanilacagi yerler:
- alert aciklamasi
- case summary
- investigator not taslagi
- suspicious pattern anlatimi
- yonetici ozet raporu

Onerilen model sirasi:
- ilk prototip: `gemini-2.5-flash`
- daha kaliteli case narration gerekiyorsa: `gemini-2.5-pro`

## 8) Agent tarafinda hangi urunu kullanacagiz?

Onerilen sira:

1. Ilk test:
- `Vertex AI Studio`
- amac: prompt ve tool davranisini test etmek

2. Sonraki adim:
- `Agent Designer`
- amac: dialog/prompt/tool akisini hizli prototiplemek

3. Kalici calisma:
- `Agent Engine`
- amac: deploy edilmis analyst copilot

## 9) Bu asamada manuel olarak olusturman gerekenler

Zorunlu olanlar:
1. Vertex region'i `europe-west4` sec
2. `vertex-aml-agent` service account olustur
3. Bu hesaba su rolleri ver:
   - `Vertex AI User`
   - `BigQuery Data Viewer`
   - `BigQuery Job User`
4. `your-project-vertex-ew4` (veya benzeri) bucket'ini olustur

Istege bagli:
5. Test icin Vertex API key al

## 10) Simdilik olusturma

Simdi olusturma:
- Agent Designer agent
- Agent Engine instance
- Vertex Endpoint
- tuned model
- Vector Search index
- RAG corpus

Bunlari ancak su iki sey bittikten sonra olustur:
- final graph/case data modeli
- analyst copilot prompt/tool kontrati

## 11) Sonraki gelisme sirasi

En dogru profesyonel sira:
1. graph katmanini netlestir
2. BigQuery dashboard/view katmanini bitir
3. final PDF raporlari uret
4. Gemini tabanli analyst copilot prototipi kur
5. gerekirse Agent Engine deployment yap

## 12) Tek cumlelik karar

Su anda en dogru hareket:
- `Vertex AI Studio/Gemini icin altyapiyi hazirla`
- `kalici agent deployment'i simdilik beklet`
