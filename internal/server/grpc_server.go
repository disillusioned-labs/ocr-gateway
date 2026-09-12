package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/disillusioned-labs/ocr-gateway/internal/config"
	grpchandler "github.com/disillusioned-labs/ocr-gateway/internal/handler/grpc"
	"github.com/disillusioned-labs/ocr-gateway/internal/service/gateway"
	gatewaypb "github.com/disillusioned-labs/ocr-gateway/contract/ocr/gateway/v1"
	platformgrpc "github.com/disillusioned-labs/platform/grpc"
	"github.com/redis/go-redis/v9"
)

// GRPCServer assembles the Kontrak A listener. It serves only
// ocr.gateway.v1.DocumentGateway; the HTTP listener next to it carries
// probes only.
type GRPCServer struct {
	grpc *platformgrpc.Server
	log  *slog.Logger
	cfg  *config.Config
}

func NewGRPC(cfg *config.Config, log *slog.Logger, rdb *redis.Client, svc gateway.Service) (*GRPCServer, error) {
	registry := NewCallerRegistry(cfg.Gateway.Callers)
	limits := make(map[string]config.CallerConfig, len(cfg.Gateway.Callers))
	for _, c := range cfg.Gateway.Callers {
		limits[c.Name] = c
	}
	limiter := NewRateLimiter(rdb, log)
	metrics := NewMetrics()

	opts := []platformgrpc.Option{
		platformgrpc.WithMaxRecvMsgSize(cfg.GRPC.MaxRecvMsgSize),
		platformgrpc.WithMaxSendMsgSize(cfg.GRPC.MaxSendMsgSize),
		platformgrpc.WithMaxHeaderSize(cfg.GRPC.MaxHeaderSize),
		platformgrpc.WithLogger(log),
		platformgrpc.WithUnaryServerInterceptor(UnaryAuth(registry, limits, limiter, metrics, log)),
	}
	if cfg.GRPC.TLS.Enabled {
		tlsConfig, err := platformgrpc.NewTLSConfig(
			cfg.GRPC.TLS.CAFile,
			cfg.GRPC.TLS.CertFile,
			cfg.GRPC.TLS.KeyFile,
			cfg.GRPC.TLS.ServerName,
			cfg.GRPC.TLS.MutualTLS,
		)
		if err != nil {
			return nil, fmt.Errorf("build grpc server TLS config: %w", err)
		}
		opts = append(opts, platformgrpc.WithTLS(tlsConfig))
	}

	grpcServer, err := platformgrpc.NewServer(opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc server: %w", err)
	}

	gatewayServer := grpchandler.NewGatewayServer(svc, log)
	gatewaypb.RegisterDocumentGatewayServer(grpcServer.GRPC(), gatewayServer)

	return &GRPCServer{grpc: grpcServer, log: log, cfg: cfg}, nil
}

// BeginDrain flips the gRPC health to NOT_SERVING before Shutdown, so
// callers stop being routed here while in-flight RPCs drain.
func (s *GRPCServer) BeginDrain() {
	s.grpc.Health().SetNotServing("")
}

// Start blocks until the listener fails or Shutdown is called.
func (s *GRPCServer) Start() error {
	addr := fmt.Sprintf(":%d", s.cfg.GRPC.ServerPort)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	s.log.Info("grpc server listening", "addr", addr)
	if err := s.grpc.Serve(listener); err != nil {
		return fmt.Errorf("grpc server: %w", err)
	}
	return nil
}

// Shutdown drains in-flight RPCs until ctx expires, then closes.
func (s *GRPCServer) Shutdown(ctx context.Context) error {
	s.log.Info("shutting down grpc server")
	return s.grpc.Stop(ctx)
}
