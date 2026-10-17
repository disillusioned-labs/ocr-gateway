package gateway

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/disillusioned-labs/ocr-gateway/internal/contract"
	ocrpb "github.com/disillusioned-labs/platform/contract/ocr"
)

func TestSubmitMapsVerifiedCallerAndForwardsEngineErrors(t *testing.T) {
	engineError := status.Error(codes.ResourceExhausted, "engine queue is full")
	client := &fakeOCRClient{submitErr: engineError}
	service := NewService(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := WithCaller(context.Background(), Caller{
		Name:     "expense",
		DocTypes: map[string]string{"receipt": "receipt@1"},
	})
	input := SubmitInput{
		IdempotencyKey: "key-1",
		ExternalRef:    "expense-doc-1",
		DocType:        "receipt",
		Source:         FileSource{Bucket: "receipts", StoragePath: "org/doc.jpg", SizeBytes: 1200, DeclaredMime: "image/jpeg"},
	}

	if _, err := service.Submit(ctx, input); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Submit error = %v, want original RESOURCE_EXHAUSTED", err)
	}
	want := contract.SubmitInput{
		IdempotencyKey: input.IdempotencyKey,
		ExternalRef:    input.ExternalRef,
		SchemaID:       "receipt@1",
		CallerID:       "expense",
		Bucket:         input.Source.Bucket,
		StoragePath:    input.Source.StoragePath,
		SizeBytes:      input.Source.SizeBytes,
		DeclaredMime:   input.Source.DeclaredMime,
	}
	if client.submitCalls != 1 || client.submitInput != want {
		t.Fatalf("engine call = %+v (calls=%d), want %+v (calls=1)", client.submitInput, client.submitCalls, want)
	}
}

func TestSubmitRejectsUnknownDocTypeBeforeCallingEngine(t *testing.T) {
	client := &fakeOCRClient{}
	service := NewService(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := WithCaller(context.Background(), Caller{Name: "expense", DocTypes: map[string]string{"receipt": "receipt@1"}})
	_, err := service.Submit(ctx, SubmitInput{
		IdempotencyKey: "key-1",
		ExternalRef:    "expense-doc-1",
		DocType:        "passport",
		Source:         FileSource{Bucket: "receipts", StoragePath: "org/doc.jpg"},
	})
	if err != ErrUnknownDocType {
		t.Fatalf("Submit error = %v, want ErrUnknownDocType", err)
	}
	if client.submitCalls != 0 {
		t.Fatalf("engine submit calls = %d, want 0", client.submitCalls)
	}
}

type fakeOCRClient struct {
	submitInput contract.SubmitInput
	submitCalls int
	submitErr   error
}

func (c *fakeOCRClient) SubmitDocument(_ context.Context, input contract.SubmitInput) (*ocrpb.SubmitDocumentResponse, error) {
	c.submitInput = input
	c.submitCalls++
	return nil, c.submitErr
}

func (*fakeOCRClient) GetDocument(context.Context, string) (*ocrpb.GetDocumentResponse, error) {
	return nil, nil
}

var _ contract.Client = (*fakeOCRClient)(nil)
