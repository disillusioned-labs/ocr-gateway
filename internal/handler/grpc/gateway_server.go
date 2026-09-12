// Package grpc holds the Kontrak A gRPC handler: it maps wire requests to the
// gateway service and service errors to gRPC statuses.
package grpc

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/disillusioned-labs/ocr-gateway/internal/service/gateway"
	gatewaypb "github.com/disillusioned-labs/ocr-gateway/contract/ocr/gateway/v1"
	ocrpb "github.com/disillusioned-labs/platform/contract/ocr"
	platformerrors "github.com/disillusioned-labs/platform/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GatewayServer implements Kontrak A (ocr.gateway.v1.DocumentGateway).
type GatewayServer struct {
	gatewaypb.UnimplementedDocumentGatewayServer
	svc gateway.Service
	log *slog.Logger
}

func NewGatewayServer(svc gateway.Service, log *slog.Logger) *GatewayServer {
	return &GatewayServer{svc: svc, log: log}
}

// SubmitDocument validates and forwards one submit. The only gateway-originated
// failures are mapping failures (unknown doc_type, missing fields); everything
// else arrives as ocr's own status, unchanged.
func (s *GatewayServer) SubmitDocument(ctx context.Context, req *gatewaypb.SubmitDocumentRequest) (*gatewaypb.SubmitDocumentResponse, error) {
	if req.GetSource() == nil {
		return nil, status.Error(codes.InvalidArgument, "source is required")
	}

	out, err := s.svc.Submit(ctx, gateway.SubmitInput{
		IdempotencyKey: req.GetIdempotencyKey(),
		ExternalRef:    req.GetExternalRef(),
		DocType:        req.GetDocType(),
		Source: gateway.FileSource{
			Bucket:       req.GetSource().GetBucket(),
			StoragePath:  req.GetSource().GetStoragePath(),
			SizeBytes:    req.GetSource().GetSizeBytes(),
			DeclaredMime: req.GetSource().GetDeclaredMime(),
		},
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &gatewaypb.SubmitDocumentResponse{
		DocumentId: out.DocumentID,
		Status:     convertSubmitStatus(out.Status),
	}, nil
}

// GetDocument forwards a poll to ocr untouched - the reconciliation sweep and
// the client both read the same answer ocr would give.
func (s *GatewayServer) GetDocument(ctx context.Context, req *gatewaypb.GetDocumentRequest) (*gatewaypb.GetDocumentResponse, error) {
	out, err := s.svc.Get(ctx, gateway.GetInput{DocumentID: req.GetDocumentId()})
	if err != nil {
		return nil, toStatus(err)
	}
	return convertGetDocumentResponse(out.Response), nil
}

// convertSubmitStatus recasts the shared enum. Kontrak A and Kontrak B declare
// the same DocumentStatus values in the same order (the doc keeps them
// byte-identical on purpose); Go sees two types because the protos are
// compiled per package.
func convertSubmitStatus(in ocrpb.DocumentStatus) gatewaypb.DocumentStatus {
	return gatewaypb.DocumentStatus(int32(in))
}

// convertGetDocumentResponse copies ocr's answer field-for-field. This is the
// one place the payload is "touched" - and it is deliberately a pure reshuffle
// between two byte-identical shapes, never an interpretation.
func convertGetDocumentResponse(in *ocrpb.GetDocumentResponse) *gatewaypb.GetDocumentResponse {
	if in == nil {
		return nil
	}
	out := &gatewaypb.GetDocumentResponse{
		DocumentId: in.GetDocumentId(),
		Status:     convertSubmitStatus(in.GetStatus()),
	}
	if r := in.GetResult(); r != nil {
		result := &gatewaypb.DocumentResult{
			SchemaVersion: r.GetSchemaVersion(),
			AvgConfidence: r.GetAvgConfidence(),
		}
		for _, f := range r.GetFields() {
			field := &gatewaypb.ExtractedField{
				Name:       f.GetName(),
				Value:      f.GetValue(),
				Amount:     f.GetAmount(),
				Currency:   f.GetCurrency(),
				Confidence: f.GetConfidence(),
				Status:     gatewaypb.FieldStatus(int32(f.GetStatus())),
				Page:       f.GetPage(),
			}
			if b := f.GetBbox(); b != nil {
				field.Bbox = &gatewaypb.BoundingBox{
					X1: b.GetX1(), Y1: b.GetY1(), X2: b.GetX2(), Y2: b.GetY2(),
				}
			}
			result.Fields = append(result.Fields, field)
		}
		for _, i := range r.GetIssues() {
			result.Issues = append(result.Issues, &gatewaypb.Issue{
				Code:   i.GetCode(),
				Field:  i.GetField(),
				Detail: i.GetDetail(),
			})
		}
		out.Result = result
	}
	if e := in.GetError(); e != nil {
		out.Error = &gatewaypb.Error{Code: e.GetCode(), Message: e.GetMessage()}
	}
	return out
}

// toStatus maps a service error to a gRPC status. Engine errors (already
// carrying a gRPC status from the contract adapter) pass through with their
// meaning intact.
func toStatus(err error) error {
	var svcErr *platformerrors.Error
	if !errors.As(err, &svcErr) {
		return err
	}
	code := fromHTTPStatus(svcErr.Status)
	return status.Error(code, svcErr.Message)
}

// fromHTTPStatus translates the domain error's HTTP status into the gRPC
// vocabulary. The gateway declares errors in HTTP terms (the workspace-wide
// error type) and converts only here, at the gRPC boundary.
func fromHTTPStatus(httpStatus int) codes.Code {
	switch httpStatus {
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusConflict:
		return codes.AlreadyExists
	case http.StatusRequestTimeout:
		return codes.DeadlineExceeded
	case http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case http.StatusNotImplemented:
		return codes.Unimplemented
	case http.StatusServiceUnavailable:
		return codes.Unavailable
	case http.StatusGatewayTimeout:
		return codes.DeadlineExceeded
	default:
		return codes.Unknown
	}
}
