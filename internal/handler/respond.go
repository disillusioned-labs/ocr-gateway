// Package handler holds the gateway's HTTP helpers. The gateway's only HTTP
// surface is the probe endpoints, but the discipline is the same as every
// other service in the workspace: marshal into a buffer BEFORE touching the
// ResponseWriter, so a mid-marshal failure can never ship a truncated body
// under a committed 200.
package handler

import (
	"encoding/json"
	"net/http"
)

// WriteJSON marshals v and writes it with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	buf, err := json.Marshal(v)
	if err != nil {
		// Nothing has been written yet, so a 500 is still shippable.
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buf)
}

// WriteError writes a plain {"error": {"code", "message"}} envelope.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	WriteJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}
