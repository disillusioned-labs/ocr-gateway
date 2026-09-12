package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/disillusioned-labs/ocr-gateway/internal/handler/health"
	"github.com/go-chi/chi/v5"
)

// HTTPServer carries the probes for the grpc binary. There is no public API
// here and never will be: the contract is Kontrak A over gRPC, and a second
// data path on the HTTP port would be an unauthenticated side door.
type HTTPServer struct {
	srv *http.Server
	h   *health.Handler
	log *slog.Logger
}

// NewHTTPServer assembles the probe listener. Required dependencies fail
// readiness; optional ones are reported only.
func NewHTTPServer(port int, required, optional map[string]health.Pinger, log *slog.Logger) *HTTPServer {
	h := health.NewHandler(required, optional)

	r := chi.NewRouter()
	r.Get("/healthz", h.Liveness)
	r.Get("/readyz", h.Readiness)

	return &HTTPServer{
		srv: &http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      r,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		h:   h,
		log: log,
	}
}

// PingFunc adapts a bare ping function to health.Pinger - used for
// dependencies that may legitimately be absent (optional-mode Redis), where
// the closure itself reports the degradation.
type PingFunc func(ctx context.Context) error

func (f PingFunc) Ping(ctx context.Context) error { return f(ctx) }

// Start blocks until the listener fails or Shutdown is called.
func (s *HTTPServer) Start() error {
	s.log.Info("probe server listening", "addr", s.srv.Addr)
	if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("probe listener: %w", err)
	}
	return nil
}

// BeginDrain fails /readyz while traffic is still served, so an orchestrator
// can pull this instance before the gRPC listener closes.
func (s *HTTPServer) BeginDrain() { s.h.BeginDrain() }

// Shutdown stops the probe listener.
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
