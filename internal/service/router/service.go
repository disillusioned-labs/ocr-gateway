package router

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/disillusioned-labs/ocr-gateway/internal/constant"
	"github.com/disillusioned-labs/ocr-gateway/internal/envelope"
	"github.com/disillusioned-labs/ocr-gateway/internal/repository"
)

var tracer = otel.Tracer("service/router")

// dedupeKeyPrefix versions marks written only after the routed outbox row is
// durable; the old prefix could contain marks written before a failed insert.
const dedupeKeyPrefix = "ocr:dedupe:routed:v2:event:"

const routedEventIDNamespace = "disillusioned-labs/ocr-gateway/document.routed/v1/"

// Service routes one document.processed event to its caller's topic. It owns
// the transaction that makes "routed" durable, so it is the one service here
// that holds a repository.Store.
type Service interface {
	Handle(ctx context.Context, env envelope.Envelope, traceID string) error
}

// Options carries the plain values config resolved; the router reads no
// configuration itself.
type Options struct {
	// DedupeTTL is the Redis TTL of the event-id mark (>= 24h, per contract).
	DedupeTTL time.Duration
	// RoutedTopicTemplate renders the destination topic from the caller_id.
	RoutedTopicTemplate string
	// KnownCaller reports whether a caller_id is registered. An event from
	// an unregistered caller is dead-lettered, not routed to a surprise topic.
	KnownCaller func(callerID string) bool
}

type routerService struct {
	repo repository.Store
	rdb  *redis.Client
	opts Options
	log  *slog.Logger
}

func NewService(repo repository.Store, rdb *redis.Client, opts Options, log *slog.Logger) Service {
	return &routerService{repo: repo, rdb: rdb, opts: opts, log: log}
}

// Handle runs the four-step algorithm from docs/reference/api-ocr-gateway.md:
// dedupe by event_id, read caller_id, one transaction writing the routed
// outbox row, and nothing else - the offset commit happens in the consumer
// loop only after this returns nil.
func (s *routerService) Handle(ctx context.Context, env envelope.Envelope, traceID string) error {
	ctx, span := tracer.Start(ctx, "RouterService.Handle")
	defer span.End()

	span.SetAttributes(
		attribute.String("router.event_id", env.EventID),
		attribute.String("router.caller_id", env.CallerID),
	)

	// Validate before touching Redis so permanent contract errors can reach DLQ
	// even when Redis is unavailable.
	if !s.opts.KnownCaller(env.CallerID) {
		span.SetStatus(codes.Error, "unknown caller")
		return &PermanentError{Reason: fmt.Sprintf("caller_id %q is not registered", env.CallerID)}
	}

	documentID, err := uuid.Parse(env.DocumentID)
	if err != nil {
		span.SetStatus(codes.Error, "document_id not a uuid")
		return &PermanentError{Reason: fmt.Sprintf("document_id %q is not a uuid", env.DocumentID)}
	}
	sourceEventID, err := uuid.Parse(env.EventID)
	if err != nil {
		span.SetStatus(codes.Error, "event_id not a uuid")
		return &PermanentError{Reason: fmt.Sprintf("event_id %q is not a uuid", env.EventID)}
	}

	routedID := uuid.NewSHA1(uuid.NameSpaceURL, append([]byte(routedEventIDNamespace), sourceEventID[:]...))
	if s.rdb == nil {
		return fmt.Errorf("dedupe redis unavailable")
	}

	// Redis is only a fast path. Its marker is written after the outbox commit;
	// the deterministic outbox ID remains the durable dedupe key if Redis fails.
	key := dedupeKeyPrefix + env.EventID
	exists, err := s.rdb.Exists(ctx, key).Result()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "dedupe check failed")
		return fmt.Errorf("dedupe %s: %w", env.EventID, err)
	}
	if exists > 0 {
		s.log.InfoContext(ctx, "duplicate event delivery; skipping", "event_id", env.EventID)
		return nil
	}

	// The four deltas (and only those): new event_id, document.routed,
	// ocr-gateway. Value fields - external_ref, caller_id, idempotency_key,
	// data, error - travel untouched.
	routed := env
	routed.EventType = constant.EventTypeDocumentRouted
	routed.Producer = constant.ProducerName
	routed.EventID = routedID.String()
	payload, err := routed.Marshal()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal routed payload")
		return fmt.Errorf("marshal routed payload: %w", err)
	}

	// Step 4: the source event deterministically names its outbox row. The
	// primary key makes retries idempotent even if Redis is lost or unavailable.
	err = s.repo.ExecTx(ctx, func(q repository.Querier) error {
		_, err := q.CreateOutboxEvent(ctx, repository.CreateOutboxEventParams{
			AggregateType: constant.AggregateType,
			AggregateID:   documentID,
			ID:            routedID,
			EventType:     constant.EventTypeDocumentRouted,
			EventVersion:  constant.EventVersion,
			Topic:         constant.RoutedTopic(s.opts.RoutedTopicTemplate, env.CallerID),
			Payload:       payload,
			TraceID:       pgtype.Text{String: traceID, Valid: traceID != ""},
		})
		return err
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "outbox insert failed")
		return fmt.Errorf("insert routed outbox event: %w", err)
	}

	if _, err := s.rdb.SetNX(ctx, key, "1", s.opts.DedupeTTL).Result(); err != nil {
		// The primary-key conflict above makes a later redelivery safe; Redis
		// failure here must not keep an already durable event uncommitted.
		s.log.WarnContext(ctx, "dedupe mark failed after durable outbox insert", "error", err, "event_id", env.EventID)
	}

	s.log.InfoContext(ctx, "event routed",
		"source_event_id", env.EventID,
		"routed_event_id", routed.EventID,
		"caller_id", env.CallerID,
		"document_id", env.DocumentID,
	)
	return nil
}
