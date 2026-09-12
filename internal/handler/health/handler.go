// Package health exposes liveness and readiness probes on the grpc binary's
// HTTP listener. The consumer and worker binaries have no HTTP surface: their
// liveness is the Kafka group membership / process uptime.
package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/disillusioned-labs/ocr-gateway/internal/handler"
	"go.opentelemetry.io/otel/trace"
)

// Pinger is satisfied by *pgxpool.Pool; other dependencies adapt to it in
// the server package.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Handler serves the /healthz and /readyz probes.
type Handler struct {
	// required dependencies fail readiness (503) when down.
	required map[string]Pinger
	// optional dependencies are reported but never fail readiness.
	optional map[string]Pinger
	// draining makes readiness fail while the process is shutting down.
	// Read on every probe and written once from the shutdown path, hence atomic.
	draining atomic.Bool
}

func NewHandler(required, optional map[string]Pinger) *Handler {
	return &Handler{required: required, optional: optional}
}

// BeginDrain makes /readyz fail from now on, without touching liveness.
//
// It is called before shutdown starts so an orchestrator can take this
// instance out of rotation while it is still able to finish in-flight work.
func (h *Handler) BeginDrain() { h.draining.Store(true) }

// Liveness always returns 200 while the process is up, including while
// draining: a liveness failure tells the orchestrator to kill the process,
// which is the opposite of what a graceful shutdown wants.
func (h *Handler) Liveness(w http.ResponseWriter, _ *http.Request) {
	handler.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// probeParent stops the ping spans that otelpgx and redisotel create with no
// opt-out: under the parentbased_* samplers, a child of an unsampled parent is
// dropped. Must be valid, or ParentBased falls back to the root sampler. IDs
// are arbitrary; nothing is exported under them.
var probeParent = trace.NewSpanContext(trace.SpanContextConfig{
	TraceID: trace.TraceID{0x70, 0x72, 0x6f, 0x62, 0x65, 1},
	SpanID:  trace.SpanID{0x70, 0x72, 0x6f, 0x62, 0x65, 1},
})

func unsampled(ctx context.Context) context.Context {
	return trace.ContextWithSpanContext(ctx, probeParent)
}

// Readiness returns 503 while draining, or when a required dependency fails
// its ping.
func (h *Handler) Readiness(w http.ResponseWriter, r *http.Request) {
	if h.draining.Load() {
		handler.WriteJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "draining",
		})
		return
	}

	ctx, cancel := context.WithTimeout(unsampled(r.Context()), 3*time.Second)
	defer cancel()

	status := http.StatusOK
	checks := make(map[string]string, len(h.required)+len(h.optional))

	for name, dep := range h.required {
		if err := dep.Ping(ctx); err != nil {
			checks[name] = "down: " + err.Error()
			status = http.StatusServiceUnavailable
		} else {
			checks[name] = "up"
		}
	}
	for name, dep := range h.optional {
		if err := dep.Ping(ctx); err != nil {
			checks[name] = "degraded: " + err.Error()
		} else {
			checks[name] = "up"
		}
	}

	handler.WriteJSON(w, status, map[string]any{"checks": checks})
}
