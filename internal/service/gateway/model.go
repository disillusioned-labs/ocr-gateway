package gateway

import (
	"context"

	ocrpb "github.com/disillusioned-labs/platform/contract/ocr"
)

// Caller is one registered service the gateway authenticates (from the
// GATEWAY_CALLERS registry). The gRPC interceptor resolves the presented API
// key to a Caller and stores it in the request context.
type Caller struct {
	// Name is the caller_id forwarded to ocr and echoed in every event; it
	// also selects the routed topic.
	Name string
	// DocTypes maps the business name ("receipt") to the engine's schema_id
	// ("receipt@1"). Submitting an unknown doc_type is INVALID_ARGUMENT.
	DocTypes map[string]string
}

type callerKeyType struct{}

// WithCaller stores the authenticated caller in ctx.
func WithCaller(ctx context.Context, caller Caller) context.Context {
	return context.WithValue(ctx, callerKeyType{}, caller)
}

// CallerFromContext returns the authenticated caller and whether one was set.
// Its absence inside an RPC means the interceptor did not run - an internal
// misconfiguration, never a client problem.
func CallerFromContext(ctx context.Context) (Caller, bool) {
	caller, ok := ctx.Value(callerKeyType{}).(Caller)
	return caller, ok
}

// SubmitInput is one Kontrak A submit, mapped from the wire request.
type SubmitInput struct {
	IdempotencyKey string
	ExternalRef    string
	DocType        string
	Source         FileSource
}

// FileSource mirrors Kontrak A/B's FileSource. Bucket + path only - the file
// bytes never travel through the gateway.
type FileSource struct {
	Bucket       string
	StoragePath  string
	SizeBytes    int64
	DeclaredMime string
}

// SubmitOutput is the async job reference. Status is always QUEUED; anything
// else is an error path.
type SubmitOutput struct {
	DocumentID string
	Status     ocrpb.DocumentStatus
}

// GetInput is one GetDocument poll (client polling or reconciliation sweep).
type GetInput struct {
	DocumentID string
}

// GetOutput is ocr's answer, untouched. The result and error travel through
// without the gateway interpreting them - their meaning is Kontrak B's.
type GetOutput struct {
	Response *ocrpb.GetDocumentResponse
}
