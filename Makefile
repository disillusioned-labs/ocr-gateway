.PHONY: run-grpc run-consumer run-worker build lint tidy vuln sqlc sqlc-diff migrate-new migrate-up migrate-down migrate-status proto buf-lint docker-up docker-down docker-build help

# Tool versions are pinned so local runs and CI can never drift. Bump here and
# in .github/workflows/ci.yml together. They are kept out of go.mod on purpose:
# sqlc's dependency tree (wazero, the tidb parser, cel-go) would otherwise be
# downloaded by the Dockerfile's `go mod download` on every image build.
GOOSE_VERSION ?= v3.27.3
SQLC_VERSION  ?= v1.30.0
VULN_VERSION  ?= v1.1.4
BUF_VERSION   ?= v1.50.0

GOOSE := go run github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION)
SQLC  := CGO_ENABLED=0 go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
BUF   := go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

# Local development database (created by scripts/init-db.sql at the workspace
# root, together with the ocr_gateway_app role). Override to reach a different
# instance:
#   make migrate-up DB_DSN=postgres://ocr_gateway_app:devpassword@localhost:5433/ocr_gateway?sslmode=disable
DB_DSN ?= postgres://ocr_gateway_app:devpassword@localhost:5432/ocr_gateway?sslmode=disable

# Build provenance, matching the Dockerfile's ldflags so a local binary reports
# the same fields as a container one.
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X github.com/disillusioned-labs/ocr-gateway/internal/app.version=$(VERSION) \
	-X github.com/disillusioned-labs/ocr-gateway/internal/app.commit=$(COMMIT) \
	-X github.com/disillusioned-labs/ocr-gateway/internal/app.buildDate=$(BUILD_DATE)

run-grpc: ## Run the Kontrak A gRPC server locally
	go run ./cmd/grpc

run-consumer: ## Run the document.processed consumer locally
	go run ./cmd/consumer

run-worker: ## Run the outbox publisher locally
	go run ./cmd/worker

build: ## Build all binaries (grpc, consumer, worker) into ./bin
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/grpc ./cmd/grpc
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/consumer ./cmd/consumer
	go build -trimpath -ldflags="$(LDFLAGS)" -o bin/worker ./cmd/worker

lint: ## Run golangci-lint
	golangci-lint run

tidy: ## go mod tidy
	go mod tidy

vuln: ## Scan dependencies for known vulnerabilities
	go run golang.org/x/vuln/cmd/govulncheck@$(VULN_VERSION) ./...

sqlc: ## Regenerate type-safe query code
	$(SQLC) generate

sqlc-diff: ## Fail if generated code is stale (same check CI runs)
	$(SQLC) diff

migrate-new: ## Create a new migration: make migrate-new name=add_foo
	$(GOOSE) -dir db/migrations create $(name) sql

migrate-up: ## Apply migrations to the local database
	$(GOOSE) -dir db/migrations postgres "$(DB_DSN)" up

migrate-down: ## Roll back one migration
	$(GOOSE) -dir db/migrations postgres "$(DB_DSN)" down

migrate-status: ## Show which migrations are applied
	$(GOOSE) -dir db/migrations postgres "$(DB_DSN)" status

proto: ## Regenerate Go code from proto/ocr/gateway/v1
	$(BUF) generate

buf-lint: ## Lint the proto contracts
	$(BUF) lint

docker-up: ## Start the local application stack (needs the infra networks)
	docker compose up -d

docker-down: ## Stop the local application stack
	docker compose down

docker-build: ## Build the production application image
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-t ocr-gateway:$(VERSION) \
		-t ocr-gateway:latest \
		.

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'
