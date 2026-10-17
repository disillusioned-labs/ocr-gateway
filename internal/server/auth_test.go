package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	gatewaypb "github.com/disillusioned-labs/ocr-gateway/contract/ocr/gateway/v1"
	"github.com/disillusioned-labs/ocr-gateway/internal/config"
	grpchandler "github.com/disillusioned-labs/ocr-gateway/internal/handler/grpc"
	"github.com/disillusioned-labs/ocr-gateway/internal/service/gateway"
	ocrpb "github.com/disillusioned-labs/platform/contract/ocr"
)

func TestUnaryAuthLimitsSubmitOnlyAndAuthenticatesEveryMethod(t *testing.T) {
	redisServer := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	const apiKey = "local-test-key"
	caller := config.CallerConfig{
		Name:       "expense",
		APIKeyHash: HashAPIKey(apiKey),
		RateLimit:  config.RateLimitConfig{Requests: 1, Window: time.Hour},
		DocTypes:   map[string]string{"receipt": "receipt@1"},
	}
	registry := NewCallerRegistry([]config.CallerConfig{caller})
	limits := map[string]config.CallerConfig{caller.Name: caller}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := &recordingGatewayService{}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(UnaryAuth(registry, limits, NewRateLimiter(rdb, log), &Metrics{})))
	gatewaypb.RegisterDocumentGatewayServer(grpcServer, grpchandler.NewGatewayServer(service, log))

	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	conn, err := grpc.NewClient("passthrough:///gateway-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := gatewaypb.NewDocumentGatewayClient(conn)
	authCtx := metadata.AppendToOutgoingContext(context.Background(), apiKeyHeader, apiKey)
	request := &gatewaypb.SubmitDocumentRequest{
		IdempotencyKey: "key-1",
		ExternalRef:    "expense-doc-1",
		DocType:        "receipt",
		Source:         &gatewaypb.FileSource{Bucket: "receipts", StoragePath: "org/doc.jpg"},
	}

	if _, err := client.SubmitDocument(authCtx, request); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	var headers metadata.MD
	if _, err := client.SubmitDocument(authCtx, request, grpc.Header(&headers)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("second submit error = %v, want RESOURCE_EXHAUSTED", err)
	}
	retryAfter := headers.Get(retryAfterHeader)
	if len(retryAfter) == 0 {
		t.Fatal("rate-limited response did not include retry-after")
	}
	seconds, err := strconv.Atoi(retryAfter[0])
	if err != nil || seconds < 1 {
		t.Fatalf("retry-after = %v, want a positive whole-second delay", retryAfter)
	}

	for i := 0; i < 2; i++ {
		if _, err := client.GetDocument(authCtx, &gatewaypb.GetDocumentRequest{DocumentId: "doc-1"}); err != nil {
			t.Fatalf("GetDocument %d: %v", i+1, err)
		}
	}
	badKeyCtx := metadata.AppendToOutgoingContext(context.Background(), apiKeyHeader, "wrong-key")
	if _, err := client.GetDocument(badKeyCtx, &gatewaypb.GetDocumentRequest{DocumentId: "doc-1"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated GetDocument error = %v, want UNAUTHENTICATED", err)
	}

	if service.submits != 1 || service.gets != 2 {
		t.Fatalf("backend calls = %d submits, %d gets; want 1 submit, 2 gets", service.submits, service.gets)
	}
}

func TestRateLimiterUnavailableFailsOpenWithoutPanicking(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, allowed := NewRateLimiter(nil, log).Allow(context.Background(), gateway.Caller{Name: "expense"}, config.CallerConfig{
		RateLimit: config.RateLimitConfig{Requests: 1, Window: time.Minute},
	})
	if !allowed {
		t.Fatal("request should be allowed when optional Redis is unavailable")
	}
}

type recordingGatewayService struct {
	submits int
	gets    int
}

func (s *recordingGatewayService) Submit(context.Context, gateway.SubmitInput) (gateway.SubmitOutput, error) {
	s.submits++
	return gateway.SubmitOutput{DocumentID: "doc-1", Status: ocrpb.DocumentStatus_DOCUMENT_STATUS_QUEUED}, nil
}

func (s *recordingGatewayService) Get(_ context.Context, input gateway.GetInput) (gateway.GetOutput, error) {
	s.gets++
	return gateway.GetOutput{Response: &ocrpb.GetDocumentResponse{
		DocumentId: input.DocumentID,
		Status:     ocrpb.DocumentStatus_DOCUMENT_STATUS_QUEUED,
	}}, nil
}
