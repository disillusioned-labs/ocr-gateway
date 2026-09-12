package contract

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	ocrpb "github.com/disillusioned-labs/platform/contract/ocr"
	platformgrpc "github.com/disillusioned-labs/platform/grpc"
)

var tracer = otel.Tracer("contract/ocr")

// grpcOcrClient wraps the ocr engine's gRPC connection. Unlike other adapters
// in this workspace it does NOT fall back on error: an ocr failure must reach
// the Kontrak A caller unchanged (UNAVAILABLE stays retryable,
// ALREADY_EXISTS stays a success, and swallowing any of them breaks the
// contract in both directions).
type grpcOcrClient struct {
	client ocrpb.DocumentServiceClient
	log    *slog.Logger
}

func NewGRPCOcrClient(conn *platformgrpc.Client, log *slog.Logger) Client {
	return &grpcOcrClient{
		client: ocrpb.NewDocumentServiceClient(conn.Conn()),
		log:    log,
	}
}

func (c *grpcOcrClient) SubmitDocument(ctx context.Context, in SubmitInput) (*ocrpb.SubmitDocumentResponse, error) {
	ctx, span := tracer.Start(ctx, "OcrClient.SubmitDocument")
	defer span.End()

	span.SetAttributes(
		attribute.String("ocr.caller_id", in.CallerID),
		attribute.String("ocr.schema_id", in.SchemaID),
		attribute.String("ocr.bucket", in.Bucket),
	)

	resp, err := c.client.SubmitDocument(ctx, &ocrpb.SubmitDocumentRequest{
		IdempotencyKey: in.IdempotencyKey,
		ExternalRef:    in.ExternalRef,
		SchemaId:       in.SchemaID,
		CallerId:       in.CallerID,
		Source: &ocrpb.FileSource{
			Bucket:       in.Bucket,
			StoragePath:  in.StoragePath,
			SizeBytes:    in.SizeBytes,
			DeclaredMime: in.DeclaredMime,
		},
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "ocr submit failed")
		c.log.WarnContext(ctx, "ocr submit failed", "error", err, "caller_id", in.CallerID)
		return nil, err
	}
	return resp, nil
}

func (c *grpcOcrClient) GetDocument(ctx context.Context, documentID string) (*ocrpb.GetDocumentResponse, error) {
	ctx, span := tracer.Start(ctx, "OcrClient.GetDocument")
	defer span.End()

	span.SetAttributes(attribute.String("ocr.document_id", documentID))

	resp, err := c.client.GetDocument(ctx, &ocrpb.GetDocumentRequest{DocumentId: documentID})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "ocr get failed")
		c.log.WarnContext(ctx, "ocr get failed", "error", err, "document_id", documentID)
		return nil, err
	}
	return resp, nil
}
