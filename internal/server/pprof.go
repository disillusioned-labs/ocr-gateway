package server

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"runtime"
)

// NewPprofServer builds the profiling listener, or nil when disabled. Being
// off is an attack-surface decision, not a performance one: pprof samples
// nothing until an endpoint is requested, but the socket it listens on is a
// door, so there is deliberately no bind-host knob - the listener is hardcoded
// to loopback and is meant to be reached with kubectl port-forward, never by
// widening the bind.
func NewPprofServer(enabled bool, port int, log *slog.Logger) *http.Server {
	if !enabled {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	log.Info("pprof enabled", "addr", addr, "goroutines", runtime.NumGoroutine())
	return &http.Server{Addr: addr, Handler: mux}
}
