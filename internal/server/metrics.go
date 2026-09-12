package server

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Metrics is the gateway's one hand-built instrument. Everything else comes
// from libraries (otelgrpc on the server, otelpgx, redisotel); the per-caller
// submit counter is not derivable from them because the caller identity lives
// in the interceptor, not in the RPC metadata otelgrpc sees.
//
// Attributes stay bounded: caller is a registry name, status is a gRPC code -
// nothing request-shaped (id, email, trace) is ever a label.
type Metrics struct {
	requests metric.Int64Counter
}

var (
	metricsOnce sync.Once
	metricsInst *Metrics
)

// NewMetrics builds the counters once per process; every replica of the
// server shares the instrument.
func NewMetrics() *Metrics {
	metricsOnce.Do(func() {
		meter := otel.Meter("github.com/disillusioned-labs/ocr-gateway/internal/server")
		counter, err := meter.Int64Counter(
			"grpc.gateway.requests",
			metric.WithDescription("Kontrak A RPCs by caller and gRPC status"),
		)
		if err != nil {
			// The no-op meter never errors; a real one only does on name
			// violations, which are compile-time constants here.
			counter = nil
		}
		metricsInst = &Metrics{requests: counter}
	})
	return metricsInst
}

// Requests counts one RPC outcome. caller is empty for requests rejected
// before authentication (unknown key: never a label, or every typo is a series).
func (m *Metrics) Requests(ctx context.Context, caller string, err error) {
	if m == nil || m.requests == nil {
		return
	}
	attrs := []attribute.KeyValue{attribute.String("status", statusCode(err))}
	if caller != "" {
		attrs = append(attrs, attribute.String("caller", caller))
	}
	m.requests.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// statusCode renders the final gRPC code of an RPC outcome.
func statusCode(err error) string {
	if err == nil {
		return codes.OK.String()
	}
	return status.Code(err).String()
}
