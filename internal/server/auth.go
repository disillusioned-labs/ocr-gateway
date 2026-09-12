package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/disillusioned-labs/ocr-gateway/internal/config"
	"github.com/disillusioned-labs/ocr-gateway/internal/service/gateway"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// apiKeyHeader carries the caller's API key on every Kontrak A RPC.
const apiKeyHeader = "x-api-key"

// retryAfterHeader carries the suggested wait on RESOURCE_EXHAUSTED, in whole
// seconds, as response metadata per the gRPC convention.
const retryAfterHeader = "retry-after"

// CallerRegistry holds the registered callers, keyed by the SHA-256 digest of
// their API key. The raw key is never stored: the interceptor hashes what was
// presented and compares digests, so a leaked config leaks nothing usable.
type CallerRegistry struct {
	byKeyHash map[string]gateway.Caller
}

// NewCallerRegistry indexes the configured callers. Boot fails upstream when
// the registry is empty - a gateway nobody can authenticate to must not start.
func NewCallerRegistry(callers []config.CallerConfig) *CallerRegistry {
	byKeyHash := make(map[string]gateway.Caller, len(callers))
	for _, c := range callers {
		byKeyHash[c.APIKeyHash] = gateway.Caller{
			Name:     c.Name,
			DocTypes: c.DocTypes,
		}
	}
	return &CallerRegistry{byKeyHash: byKeyHash}
}

// Lookup resolves a presented API key to its caller. The digest comparison is
// the map key itself; the key never appears in logs or errors.
func (r *CallerRegistry) Lookup(apiKey string) (gateway.Caller, bool) {
	caller, ok := r.byKeyHash[HashAPIKey(apiKey)]
	if !ok {
		return gateway.Caller{}, false
	}
	return caller, true
}

// HashAPIKey digests a presented key the same way the registry was built.
func HashAPIKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}

// KnownCaller reports whether a caller_id is registered. The router uses this
// to pick the routed topic; an event whose caller_id is not registered is a
// contract violation, not a topic to invent.
func (r *CallerRegistry) KnownCaller(callerID string) bool {
	for _, c := range r.byKeyHash {
		if c.Name == callerID {
			return true
		}
	}
	return false
}

// RateLimiter is the per-caller submit budget: a Redis fixed window keyed by
// caller name and window start. Counts are shared across replicas, so unlike
// an in-process limiter N replicas do not multiply the budget.
type RateLimiter struct {
	rdb *redis.Client
	log *slog.Logger
}

func NewRateLimiter(rdb *redis.Client, log *slog.Logger) *RateLimiter {
	return &RateLimiter{rdb: rdb, log: log}
}

// Allow reports whether caller may submit now, and if not, how long to wait.
// A Redis hiccup fails OPEN: a brief window without a cap beats a hard outage
// of submit for every caller at once, and the engine's own queue bound stays
// in place regardless.
func (l *RateLimiter) Allow(ctx context.Context, caller gateway.Caller, limit config.CallerConfig) (time.Duration, bool) {
	window := limit.RateLimit.Window
	windowStart := time.Now().UnixNano() / int64(window)
	key := fmt.Sprintf("ocr:ratelimit:%s:%d", caller.Name, windowStart)

	n, err := l.rdb.Incr(ctx, key).Result()
	if err != nil {
		l.log.WarnContext(ctx, "rate limit check failed; allowing request", "error", err, "caller", caller.Name)
		return 0, true
	}
	if n == 1 {
		// First hit in this window: set the TTL so dead callers' counters
		// do not accumulate. Window*2 comfortably covers clock skew.
		l.rdb.Expire(ctx, key, 2*window)
	}
	if n > int64(limit.RateLimit.Requests) {
		elapsed := time.Duration(time.Now().UnixNano() - windowStart*int64(window))
		return window - elapsed, false
	}
	return 0, true
}

// UnaryAuth returns the Kontrak A interceptor: authenticate by API key, rate
// limit per caller, stamp the caller into the context, and count the outcome.
func UnaryAuth(reg *CallerRegistry, limits map[string]config.CallerConfig, limiter *RateLimiter, metrics *Metrics, log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		caller, rejected, err := authenticate(ctx, reg, limits, limiter)
		if rejected {
			metrics.Requests(ctx, "", err)
			return nil, err
		}

		ctx = gateway.WithCaller(ctx, caller)
		resp, err := handler(ctx, req)
		metrics.Requests(ctx, caller.Name, err)
		return resp, err
	}
}

// authenticate performs the two gate checks; rejected=true means the request
// dies here and err is the terminal status.
func authenticate(ctx context.Context, reg *CallerRegistry, limits map[string]config.CallerConfig, limiter *RateLimiter) (gateway.Caller, bool, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return gateway.Caller{}, true, status.Error(codes.Unauthenticated, "missing metadata")
	}
	keys := md.Get(apiKeyHeader)
	if len(keys) == 0 || keys[0] == "" {
		return gateway.Caller{}, true, status.Error(codes.Unauthenticated, "missing x-api-key")
	}

	caller, known := reg.Lookup(keys[0])
	if !known {
		return gateway.Caller{}, true, status.Error(codes.Unauthenticated, "unknown api key")
	}

	limit, ok := limits[caller.Name]
	if !ok {
		return gateway.Caller{}, true, status.Error(codes.Internal, "caller rate limit not configured")
	}

	retryAfter, allowed := limiter.Allow(ctx, caller, limit)
	if !allowed {
		// Response metadata, not the message: consumers branch on the status
		// code and read retry-after, never the human text.
		_ = grpc.SetHeader(ctx, metadata.Pairs(retryAfterHeader, strconv.Itoa(int(retryAfter.Seconds()))))
		return gateway.Caller{}, true, status.Errorf(codes.ResourceExhausted,
			"rate limit exceeded for caller %q", caller.Name)
	}
	return caller, false, nil
}
