package gateway

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/disillusioned-labs/ocr-gateway/internal/contract"
	platformerrors "github.com/disillusioned-labs/platform/errors"
)

var tracer = otel.Tracer("service/gateway")

// Gateway-originated errors. Everything else a caller sees is ocr's own gRPC
// status, forwarded unchanged - the gateway does not relabel engine errors.
var (
	// ErrUnknownDocType: the doc_type is not registered for this caller. No
	// retry - the caller's request or the gateway's mapping is wrong.
	ErrUnknownDocType = platformerrors.NewError("DOC_TYPE_UNKNOWN", 400, "unknown doc_type for this caller")
	// ErrMissingField: a required submit field is empty. ocr would reject it
	// too, but the rejection is cheaper and clearer at the door.
	ErrMissingField = platformerrors.NewError("MISSING_FIELD", 400, "required field missing")
)

// Service is the Kontrak A use case: authenticate-mapping-forward. The
// gateway stores nothing and decides nothing about documents - it maps
// doc_type to schema_id, attaches the verified caller_id, and hands the
// request to ocr.
type Service interface {
	Submit(ctx context.Context, input SubmitInput) (SubmitOutput, error)
	Get(ctx context.Context, input GetInput) (GetOutput, error)
}

type gatewayService struct {
	ocr contract.Client
	log *slog.Logger
}

// NewService builds the gateway service. It takes no repository: this use
// case never touches the gateway's database - state lives in ocr, and the
// result travels back by event.
func NewService(ocr contract.Client, log *slog.Logger) Service {
	return &gatewayService{ocr: ocr, log: log}
}

func (s *gatewayService) Submit(ctx context.Context, input SubmitInput) (SubmitOutput, error) {
	ctx, span := tracer.Start(ctx, "GatewayService.Submit")
	defer span.End()

	caller, ok := CallerFromContext(ctx)
	if !ok {
		// The auth interceptor runs on every Kontrak A RPC; reaching here
		// means the server was wired without it.
		span.SetStatus(codes.Error, "no caller in context")
		return SubmitOutput{}, errors.New("no authenticated caller in context")
	}

	if input.IdempotencyKey == "" || input.ExternalRef == "" || input.Source.Bucket == "" || input.Source.StoragePath == "" {
		span.SetStatus(codes.Error, "missing required field")
		return SubmitOutput{}, ErrMissingField
	}

	schemaID, known := caller.DocTypes[input.DocType]
	if !known {
		span.SetAttributes(attribute.String("gateway.doc_type", input.DocType))
		span.SetStatus(codes.Error, "unknown doc_type")
		return SubmitOutput{}, ErrUnknownDocType
	}

	span.SetAttributes(
		attribute.String("gateway.caller_id", caller.Name),
		attribute.String("gateway.schema_id", schemaID),
	)

	resp, err := s.ocr.SubmitDocument(ctx, contract.SubmitInput{
		IdempotencyKey: input.IdempotencyKey,
		ExternalRef:    input.ExternalRef,
		SchemaID:       schemaID,
		CallerID:       caller.Name,
		Bucket:         input.Source.Bucket,
		StoragePath:    input.Source.StoragePath,
		SizeBytes:      input.Source.SizeBytes,
		DeclaredMime:   input.Source.DeclaredMime,
	})
	if err != nil {
		// Forwarded unchanged: ocr's status IS the answer (ALREADY_EXISTS
		// carries the original document_id in its details).
		return SubmitOutput{}, err
	}
	return SubmitOutput{DocumentID: resp.GetDocumentId(), Status: resp.GetStatus()}, nil
}

func (s *gatewayService) Get(ctx context.Context, input GetInput) (GetOutput, error) {
	ctx, span := tracer.Start(ctx, "GatewayService.Get")
	defer span.End()

	if strings.TrimSpace(input.DocumentID) == "" {
		return GetOutput{}, ErrMissingField
	}

	resp, err := s.ocr.GetDocument(ctx, input.DocumentID)
	if err != nil {
		return GetOutput{}, err
	}
	return GetOutput{Response: resp}, nil
}
