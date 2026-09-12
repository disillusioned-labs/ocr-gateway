# AGENTS.md

Guidance for AI coding agents working in this repository. The workspace-wide
[AGENTS.md](../AGENTS.md) holds the shared conventions (error model, config
discipline, sqlc/goose, depguard); read it first. This file only adds what is
specific to the gateway.

## Commands

```bash
cp .env.example .env   # required first; see README for api_key_hash generation
make run-grpc          # Kontrak A server + /healthz /readyz
make run-consumer      # document.processed consumer (needs Kafka + Redis + Postgres)
make run-worker        # outbox publisher
make lint              # golangci-lint (CI pins v2.12.2 - keep local in sync)
make sqlc              # regenerate internal/repository after editing db/queries/
make proto             # buf generate (proto/ocr/gateway/v1 -> contract/ocr/gateway/v1)
make buf-lint          # proto contract lint (breaking changes fail here)
make vuln              # govulncheck at the pinned version
```

Run tests with plain `go test ./...` on this machine: `-race` fails locally
(32-bit gcc); CI covers the race detector. There are no tests yet - the
hand-written-fakes pattern from identity/expense applies when the first one
lands.

## Non-negotiable invariants

- **The gateway is provider-blind and domain-ignorant, forever.** No vendor
  code (Baidu, Paddle), no extraction logic, no field semantics ("total",
  "pajak"), no business validation. Provider switching lives inside service
  `ocr` behind `OcrProvider` (docs/explanation/keputusan-dokumentasi.md #8);
  routing-to-vendor-at-the-gateway was considered and rejected - do not
  re-litigate without reading plan-b.md.
- **The payload is never edited.** The only Kontrak transforms are
  `doc_type → schema_id` plus `caller_id` on the submit path, and the four
  routed-event deltas (`event_type`, `producer`, `event_id`, message key) on
  the consumer path. `data`/`error` travel byte-for-byte via
  `internal/envelope`'s `json.RawMessage` - do not decode them into typed
  structs here.
- **No status tables.** The gateway's database (`ocr_gateway`) has exactly one
  table, `outbox_events`. A `documents`-like state table or a caller→topic
  mapping table here is a review alarm: state lives in `ocr`'s `documents`
  and routing is stateless via `caller_id` echoed in events.
- **Offset commit ordering.** The consumer commits the Kafka offset only
  after the routed event is durable in the gateway's own outbox (one
  `ExecTx`). Publishing routed events happens exclusively through the outbox
  worker - never a direct produce from the consumer path.
- **Dedupe layers.** Redis `SET NX EX` per `event_id` is the efficiency
  layer; the requester's DB unique constraint is the truth layer. Redis
  failure in the consumer fails closed (no commit, redelivery) - a missing
  mark must not become a second routed event.
- **Errors pass through.** ocr's gRPC statuses reach Kontrak A callers
  unchanged (`contract.NewGRPCOcrClient` deliberately has no fallback). The
  gateway-originated errors are only mapping errors (`service/gateway`) and
  rate limiting (`server.UnaryAuth`). Consumers read `retry-after` metadata,
  never the message text.
- **Callers are config, not code.** `GATEWAY_CALLERS` (JSON) is the only
  registry: api_key hash, rate limit, doc_type mapping. Adding a caller
  requires a new routed topic in Kafka and nothing else; the boot fails when
  the registry is empty or a routed topic's caller is unknown.

## Structure notes

- Kontrak A proto lives in `proto/ocr/gateway/v1` (this repo owns it, per
  docs). Generated Go goes to `contract/ocr/gateway/v1` - **not** `internal/`:
  expense imports the client cross-module, and Go refuses internal imports
  across modules. Generated code is committed (same as platform/contract).
- Kontrak B proto (`document.v1`) is temporarily hosted in
  `platform/contract/ocr/` with the generated code; it moves to repo `ocr`
  when that repo exists, and this note moves with it.
- `internal/envelope` is the shared wire type between the consumer and the
  outbox worker. It is a package, not a service - `service/outbox` importing
  `service/router` would be the forbidden service→service edge.
- The gRPC binary's HTTP listener serves probes only. There is no public REST
  API and no `/metrics` (OTLP push, like every other service here).
