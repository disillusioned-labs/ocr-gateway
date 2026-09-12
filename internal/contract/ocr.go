// Package contract adapts the ocr engine's gRPC surface (Kontrak B,
// document.v1) to a Go interface the gateway service can hold without knowing
// protobuf or connection details. Errors from the engine are returned as-is:
// the gateway forwards ocr's statuses (ALREADY_EXISTS, RESOURCE_EXHAUSTED,
// UNAVAILABLE, ...) without changing their meaning.
package contract

import (
	"context"

	ocrpb "github.com/disillusioned-labs/platform/contract/ocr"
)

// SubmitInput is one Kontrak B submit, already mapped by the gateway service:
// schema_id instead of doc_type, caller_id resolved from the verified caller.
type SubmitInput struct {
	// IdempotencyKey is enforced by ocr (documents.idempotency_key UNIQUE):
	// a resubmit returns ALREADY_EXISTS with the original document_id.
	IdempotencyKey string
	// ExternalRef is the caller's business id, echoed in every event.
	ExternalRef string
	// SchemaID is the engine-side schema ("receipt@1") - ocr never sees the
	// business name.
	SchemaID string
	// CallerID is the verified caller ("expense"), echoed by ocr in events
	// and used by the gateway to route results back.
	CallerID string

	Bucket       string
	StoragePath  string
	SizeBytes    int64
	DeclaredMime string
}

// Client is the ocr engine as the gateway sees it.
type Client interface {
	SubmitDocument(ctx context.Context, in SubmitInput) (*ocrpb.SubmitDocumentResponse, error)
	GetDocument(ctx context.Context, documentID string) (*ocrpb.GetDocumentResponse, error)
}
