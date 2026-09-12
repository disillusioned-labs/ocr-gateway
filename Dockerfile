# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w \
        -X github.com/disillusioned-labs/ocr-gateway/internal/app.version=${VERSION} \
        -X github.com/disillusioned-labs/ocr-gateway/internal/app.commit=${COMMIT} \
        -X github.com/disillusioned-labs/ocr-gateway/internal/app.buildDate=${BUILD_DATE}" \
      -o /out/grpc ./cmd/grpc
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w \
        -X github.com/disillusioned-labs/ocr-gateway/internal/app.version=${VERSION} \
        -X github.com/disillusioned-labs/ocr-gateway/internal/app.commit=${COMMIT} \
        -X github.com/disillusioned-labs/ocr-gateway/internal/app.buildDate=${BUILD_DATE}" \
      -o /out/consumer ./cmd/consumer
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w \
        -X github.com/disillusioned-labs/ocr-gateway/internal/app.version=${VERSION} \
        -X github.com/disillusioned-labs/ocr-gateway/internal/app.commit=${COMMIT} \
        -X github.com/disillusioned-labs/ocr-gateway/internal/app.buildDate=${BUILD_DATE}" \
      -o /out/worker ./cmd/worker


# ---------------------------------------------------------------------------
# Runtime image
# ---------------------------------------------------------------------------

FROM gcr.io/distroless/static-debian12:nonroot AS runtime

WORKDIR /app

COPY --from=build /out/grpc ./grpc
COPY --from=build /out/consumer ./consumer
COPY --from=build /out/worker ./worker

EXPOSE 8083 9093

# One image, three roles: the entry point is the gRPC server (Kontrak A); the
# document.processed consumer and the outbox publisher run by overriding the
# command (docker run <img> /app/worker).
ENTRYPOINT ["/app/grpc"]
