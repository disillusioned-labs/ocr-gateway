package app

import (
	"context"
	"log/slog"

	"github.com/disillusioned-labs/ocr-gateway/internal/config"
	"github.com/disillusioned-labs/ocr-gateway/internal/contract"
	"github.com/disillusioned-labs/ocr-gateway/internal/service/gateway"
	platformgrpc "github.com/disillusioned-labs/platform/grpc"
)

// newOcrClient creates the gRPC client to the ocr engine (Kontrak B).
// Returns (client, cleanup, error). Caller must defer cleanup.
func newOcrClient(ctx context.Context, cfg *config.Config, log *slog.Logger) (contract.Client, func(), error) {
	opts := []platformgrpc.Option{
		platformgrpc.WithUnaryTimeout(cfg.GRPCClient.Timeout),
		platformgrpc.WithMaxRecvMsgSize(cfg.GRPCClient.MaxRecvMsgSize),
		platformgrpc.WithMaxSendMsgSize(cfg.GRPCClient.MaxSendMsgSize),
		platformgrpc.WithLogger(log),
	}

	client, err := platformgrpc.NewClient(cfg.Ocr.GRPCTarget, opts...)
	if err != nil {
		return nil, nil, err
	}

	ocrClient := contract.NewGRPCOcrClient(client, log)
	cleanup := func() { _ = client.Close() }

	return ocrClient, cleanup, nil
}

// buildDeps assembles the grpc binary's dependencies. It is the only place
// that knows concrete constructors; everything downstream takes interfaces.
func buildDeps(cfg *config.Config, ocrClient contract.Client, log *slog.Logger) (gateway.Service, error) {
	return gateway.NewService(ocrClient, log), nil
}
