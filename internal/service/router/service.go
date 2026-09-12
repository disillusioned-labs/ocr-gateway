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

// dedupeKeyPrefix namespaces the gateway's dedupe keys. The event_id space is
// shared with every other consumer on the bus; the prefix is what keeps this
// service's marks out of theirs.
const dedupeKeyPrefix = "ocr:dedupe:event:"

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

	// Step 2: dedupe. SET NX is the mark; losing the race means a duplicate
	// delivery - a no-op plus commit, never a second routed event.
	set, err := s.rdb.SetNX(ctx, dedupeKeyPrefix+env.EventID, "1", s.opts.DedupeTTL).Result()
	if err != nil {
		// Fail closed: without the mark we cannot tell fresh from duplicate.
		// The record stays uncommitted and Kafka redelivers.
		span.RecordError(err)
		span.SetStatus(codes.Error, "dedupe check failed")
		return fmt.Errorf("dedupe %s: %w", env.EventID, err)
	}
	if !set {
		s.log.InfoContext(ctx, "duplicate event delivery; skipping", "event_id", env.EventID)
		return nil
	}

	// Step 3: route by caller_id - stateless, no mapping table.
	if !s.opts.KnownCaller(env.CallerID) {
		span.SetStatus(codes.Error, "unknown caller")
		return &PermanentError{Reason: fmt.Sprintf("caller_id %q is not registered", env.CallerID)}
	}

	documentID, err := uuid.Parse(env.DocumentID)
	if err != nil {
		span.SetStatus(codes.Error, "document_id not a uuid")
		return &PermanentError{Reason: fmt.Sprintf("document_id %q is not a uuid", env.DocumentID)}
	}

	// The four deltas (and only those): new event_id, document.routed,
	// ocr-gateway. Value fields - external_ref, caller_id, idempotency_key,
	// data, error - travel untouched.
	routed := env
	routed.EventType = constant.EventTypeDocumentRouted
	routed.Producer = constant.ProducerName
	routed.EventID = uuid.NewString()
	payload, err := routed.Marshal()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "marshal routed payload")
		return fmt.Errorf("marshal routed payload: %w", err)
	}

	// Step 4: one transaction. The routed event is durable before the
	// consumer commits the Kafka offset, so there is no window where the
	// event is lost - only windows where it redelivers and dedupes. The docs'
	// "marker processed" is this row itself: with exactly one table in the
	// gateway's database, published_at on the outbox row IS the processed
	// flag - there is no second table to mark.
	err = s.repo.ExecTx(ctx, func(q repository.Querier) error {
		_, err := q.CreateOutboxEvent(ctx, repository.CreateOutboxEventParams{
			AggregateType: constant.AggregateType,
			AggregateID:   documentID,
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

	s.log.InfoContext(ctx, "event routed",
		"source_event_id", env.EventID,
		"routed_event_id", routed.EventID,
		"caller_id", env.CallerID,
		"document_id", env.DocumentID,
	)
	return nil
}
