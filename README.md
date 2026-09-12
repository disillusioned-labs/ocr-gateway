# ocr-gateway

Concierge antara service bisnis dan engine OCR. Topologi: `expense → ocr-gateway (Go) → ocr (Python)` — service bisnis **tidak pernah** memanggil `ocr` langsung, dan gateway **tidak pernah** berbicara dengan vendor OCR mana pun (provider-blind; keputusan #8, September 2026).

Source of truth kontrak:

- **Kontrak A** (publik, `ocr.gateway.v1`): [docs/reference/api-ocr-gateway.md](../docs/reference/api-ocr-gateway.md) — proto milik repo ini, di `proto/ocr/gateway/v1/gateway.proto`.
- **Kontrak B** (internal `gateway → ocr`, `document.v1`): [docs/reference/api-ocr.md](../docs/reference/api-ocr.md) — proto milik repo `ocr`; sementara di-hosting di `platform/contract/ocr/` sampai repo `ocr` ada.
- Routing, dedupe, outbox: [docs/explanation/ocr-gateway/README.md](../docs/explanation/ocr-gateway/README.md) + [routing.md](../docs/explanation/ocr-gateway/routing.md).

## Apa yang gateway lakukan (dan tidak)

Gateway adalah **kurir yang berdisiplin, bukan penyunting**:

1. **Auth per-service** — API key per caller (hash SHA-256 di `GATEWAY_CALLERS`), bukan JWT user.
2. **Rate limit per caller** — Redis fixed window; over → `RESOURCE_EXHAUSTED` + `retry-after`.
3. **Mapping `doc_type → schema_id`** — satu-satunya transformasi kontrak, ditambah `caller_id`.
4. **Dedupe event** — Redis `SET NX EX` per `event_id` (TTL ≥ 24 jam, prefix `ocr:dedupe:event:`), di `cmd/consumer`.
5. **Routing balik** — baca `caller_id` dari event `document.processed`, tulis event `document.routed` ke **outbox sendiri** (satu transaksi), worker mem-publish ke `ocr.document.routed.<service>.v1` dengan **event_id baru** dan message key `external_ref`.

Gateway tidak menyimpan status dokumen, tidak tahu bisnis (total, pajak, approval), dan tidak mengubah payload — tabel satu-satunya di database `ocr_gateway` adalah `outbox_events`. Hasil OCR sampai ke pemohon lewat event Kafka, bukan respons sinkron; `GetDocument` adalah passthrough untuk polling client dan reconciliation sweep.

## Binary

| Binary | Peran |
|---|---|
| `cmd/grpc` | Kontrak A (`DocumentGateway`) + HTTP probes `/healthz` `/readyz` |
| `cmd/consumer` | consume `ocr.document.processed.v1` → dedupe → route → outbox (commit offset setelah outbox aman) |
| `cmd/worker` | publish outbox → topik routed (batch 100 / 1s, backoff `5s × min(attempt+1, 6)`) |

## Topik Kafka

Buat lewat script generik workspace (`infra/messaging/scripts/create-topic.sh <topic> [partitions]`) — `platform/kafka` juga auto-create topik saat publish pertama, tapi pre-create dengan jumlah partisi yang disengaja lebih baik:

```bash
create-topic.sh ocr.document.processed.v1        # consume (produced by ocr)
create-topic.sh ocr.document.routed.expense.v1   # produce (per caller; tambah saat mendaftarkan caller baru)
create-topic.sh ocr.document.processed.dlq       # produce (record gagal diproses di gateway)
create-topic.sh ocr.document.routed.expense.dlq  # diproduksi expense (lane DLQ consumer-nya)
```

## Konfigurasi

Satu vocabulary: environment variable (lihat [.env.example](.env.example)). Registry caller berbentuk JSON di `GATEWAY_CALLERS` — mendaftarkan caller baru = tambah entri di situ + buat topik routed-nya di Kafka; tidak ada perubahan kode.

## Menjalankan lokal

```bash
cp .env.example .env            # lalu isi api_key_hash: printf '<api-key>' | sha256sum
psql -U postgres -h localhost -f ../scripts/init-db.sql   # sekali per mesin (buat db ocr_gateway)
make run-grpc                   # atau: make run-consumer / make run-worker
```

Engine `ocr` belum ada; sampai repo itu live, `SubmitDocument` akan menjawab `UNAVAILABLE` — perilaku yang benar secara kontrak.

## Catatan rilis

`internal/contract` mengimport `github.com/disillusioned-labs/platform/contract/ocr` (proto Kontrak B) yang **belum ada di platform v0.4.1** — build standalone (CI) baru hijau setelah platform dirilis dengan paket itu. Demikian pula `expense` yang mengimport `contract/ocr/gateway/v1` dari repo ini butuh rilis pertama `ocr-gateway` sebelum CI-nya build lagi; sampai saat itu, dev memakai `go.work` (sudah termasuk `./ocr-gateway`).
