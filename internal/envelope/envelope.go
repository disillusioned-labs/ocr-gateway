// Package envelope is the wire shape of one OCR document event, shared by the
// consumer (which parses ocr's document.processed) and the outbox worker
// (which reads back the stored routed payload to derive the Kafka key). The
// gateway is a courier: it rewrites exactly three fields (event_type, producer,
// event_id), never touches data or error, and adds nothing.
package envelope

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Envelope is the value of a document.processed / document.routed record.
// Data and Error are raw JSON so the gateway forwards them byte-for-byte
// instead of decoding into structs it has no business interpreting.
// (Source of truth: docs/reference/api-ocr.md.)
type Envelope struct {
	SchemaVersion  string          `json:"schema_version"`
	EventID        string          `json:"event_id"`
	EventType      string          `json:"event_type"`
	OccurredAt     string          `json:"occurred_at"`
	DocumentID     string          `json:"document_id"`
	ExternalRef    string          `json:"external_ref"`
	CallerID       string          `json:"caller_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Producer       string          `json:"producer"`
	Data           json.RawMessage `json:"data"`
	Error          json.RawMessage `json:"error"`
}

// present reports whether a raw JSON field carries a value (not absent, not null).
func present(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

// Parse decodes and validates a document.processed record. Tolerant Reader on
// the field level (unknown fields are ignored); anything structurally wrong -
// missing keys, unparseable timestamps, data and error not mutually exclusive -
// is a contract violation the caller dead-letters.
func Parse(value []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(value, &env); err != nil {
		return Envelope{}, fmt.Errorf("decode envelope: %w", err)
	}

	var errs []error
	if env.SchemaVersion == "" {
		errs = append(errs, errors.New("schema_version missing"))
	}
	if env.EventID == "" {
		errs = append(errs, errors.New("event_id missing"))
	}
	// event_type must be document.processed - the input contract. The gateway
	// republishes as document.routed; anything else on the consumed topic is a
	// contract violation.
	if env.EventType != "document.processed" {
		errs = append(errs, fmt.Errorf("event_type %q: want document.processed", env.EventType))
	}
	if _, err := time.Parse(time.RFC3339, env.OccurredAt); err != nil {
		errs = append(errs, fmt.Errorf("occurred_at %q: %w", env.OccurredAt, err))
	}
	if env.DocumentID == "" {
		errs = append(errs, errors.New("document_id missing"))
	}
	if env.ExternalRef == "" {
		errs = append(errs, errors.New("external_ref missing"))
	}
	if env.CallerID == "" {
		errs = append(errs, errors.New("caller_id missing"))
	}
	if env.IdempotencyKey == "" {
		errs = append(errs, errors.New("idempotency_key missing"))
	}
	if present(env.Data) && present(env.Error) {
		errs = append(errs, errors.New("data and error both present"))
	}
	if !present(env.Data) && !present(env.Error) {
		errs = append(errs, errors.New("data and error both absent"))
	}
	if err := errors.Join(errs...); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// Unmarshal decodes a stored routed payload without validating it as a
// document.processed record - the outbox worker only reads back fields the
// router itself wrote.
func Unmarshal(value []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(value, &env); err != nil {
		return Envelope{}, fmt.Errorf("decode envelope: %w", err)
	}
	return env, nil
}

// Marshal re-encodes the envelope. The router marshals after rewriting
// event_type, producer and event_id; everything else round-trips verbatim.
func (e Envelope) Marshal() ([]byte, error) {
	value, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode envelope: %w", err)
	}
	return value, nil
}
