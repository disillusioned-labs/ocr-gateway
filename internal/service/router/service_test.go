package router

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/disillusioned-labs/ocr-gateway/internal/constant"
	"github.com/disillusioned-labs/ocr-gateway/internal/envelope"
	"github.com/disillusioned-labs/ocr-gateway/internal/repository"
)

func TestHandleOnlyMarksRedisAfterDurableOutboxInsert(t *testing.T) {
	redisServer := miniredis.RunT(t)
	rdb := testRedisClient(redisServer.Addr())
	t.Cleanup(func() { _ = rdb.Close() })

	store := newFakeStore()
	store.failNext = errors.New("temporary postgres failure")
	service := newTestRouter(store, rdb)
	env := testEnvelope()
	key := dedupeKeyPrefix + env.EventID

	if err := service.Handle(context.Background(), env, ""); err == nil {
		t.Fatal("first Handle error = nil, want transient insert failure")
	}
	if exists, err := rdb.Exists(context.Background(), key).Result(); err != nil || exists != 0 {
		t.Fatalf("redis marker after failed insert = %d, error = %v; want absent", exists, err)
	}

	if err := service.Handle(context.Background(), env, ""); err != nil {
		t.Fatalf("redelivery after transient insert failure: %v", err)
	}
	if len(store.rows) != 1 || store.insertAttempts != 2 {
		t.Fatalf("outbox rows = %d, insert attempts = %d; want 1 row after 2 attempts", len(store.rows), store.insertAttempts)
	}
	if err := service.Handle(context.Background(), env, ""); err != nil {
		t.Fatalf("duplicate after redis marker: %v", err)
	}
	if store.insertAttempts != 2 {
		t.Fatalf("duplicate reached database %d times; want Redis fast-path skip", store.insertAttempts)
	}

	var stored repository.CreateOutboxEventParams
	for _, row := range store.rows {
		stored = row
	}
	var routed envelope.Envelope
	if err := json.Unmarshal(stored.Payload, &routed); err != nil {
		t.Fatal(err)
	}
	if stored.ID.String() != routed.EventID || routed.EventID == env.EventID {
		t.Fatalf("routed event ID = %q, outbox ID = %q, source ID = %q", routed.EventID, stored.ID, env.EventID)
	}
	if routed.EventType != constant.EventTypeDocumentRouted || routed.Producer != constant.ProducerName {
		t.Fatalf("routed deltas = (%q, %q), want document.routed / ocr-gateway", routed.EventType, routed.Producer)
	}
	if string(routed.Data) != string(env.Data) || routed.ExternalRef != env.ExternalRef || routed.CallerID != env.CallerID {
		t.Fatalf("routing changed pass-through fields: got data=%s external_ref=%q caller_id=%q", routed.Data, routed.ExternalRef, routed.CallerID)
	}
}

func TestHandleDoesNotLoseOutboxWhenRedisMarkFailsAfterCommit(t *testing.T) {
	redisServer := miniredis.RunT(t)
	rdb := testRedisClient(redisServer.Addr())
	t.Cleanup(func() { _ = rdb.Close() })

	store := newFakeStore()
	store.afterTx = redisServer.Close
	env := testEnvelope()
	if err := newTestRouter(store, rdb).Handle(context.Background(), env, ""); err != nil {
		t.Fatalf("Handle after durable insert and Redis outage: %v", err)
	}
	if len(store.rows) != 1 {
		t.Fatalf("outbox rows after Redis outage = %d, want 1", len(store.rows))
	}

	redisRetry := miniredis.RunT(t)
	rdbRetry := testRedisClient(redisRetry.Addr())
	t.Cleanup(func() { _ = rdbRetry.Close() })
	if err := newTestRouter(store, rdbRetry).Handle(context.Background(), env, ""); err != nil {
		t.Fatalf("redelivery after Redis recovers: %v", err)
	}
	if len(store.rows) != 1 || store.insertAttempts != 2 {
		t.Fatalf("redelivery produced %d rows after %d inserts; want one durable row", len(store.rows), store.insertAttempts)
	}
}

func TestHandleFailsClosedWhenRedisIsUnavailable(t *testing.T) {
	store := newFakeStore()
	err := newTestRouter(store, nil).Handle(context.Background(), testEnvelope(), "")
	if err == nil {
		t.Fatal("Handle error = nil, want Redis unavailable")
	}
	if store.insertAttempts != 0 {
		t.Fatalf("database insert attempts = %d, want 0", store.insertAttempts)
	}
}

func newTestRouter(store *fakeStore, rdb *goredis.Client) Service {
	return NewService(store, rdb, Options{
		DedupeTTL:           24 * time.Hour,
		RoutedTopicTemplate: "ocr.document.routed.%s.v1",
		KnownCaller:         func(name string) bool { return name == "expense" },
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func testRedisClient(addr string) *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:         addr,
		MaxRetries:   -1,
		DialTimeout:  50 * time.Millisecond,
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
	})
}

func testEnvelope() envelope.Envelope {
	return envelope.Envelope{
		SchemaVersion:  "1.0",
		EventID:        "018f1f88-2f00-7000-8000-000000000001",
		EventType:      constant.EventTypeDocumentProcessed,
		OccurredAt:     "2026-10-17T00:00:00Z",
		DocumentID:     "018f1f88-2f00-7000-8000-000000000002",
		ExternalRef:    "expense-document",
		CallerID:       "expense",
		IdempotencyKey: "018f1f88-2f00-7000-8000-000000000003",
		Producer:       "ocr",
		Data:           json.RawMessage(`{"fields":[{"name":"total","amount":12765}]}`),
	}
}

type fakeStore struct {
	rows           map[uuid.UUID]repository.CreateOutboxEventParams
	insertAttempts int
	failNext       error
	afterTx        func()
}

func newFakeStore() *fakeStore {
	return &fakeStore{rows: make(map[uuid.UUID]repository.CreateOutboxEventParams)}
}

func (s *fakeStore) ExecTx(_ context.Context, fn func(repository.Querier) error) error {
	if err := fn(s); err != nil {
		return err
	}
	if s.afterTx != nil {
		after := s.afterTx
		s.afterTx = nil
		after()
	}
	return nil
}

func (s *fakeStore) ClaimPendingOutboxEvents(context.Context, repository.ClaimPendingOutboxEventsParams) ([]repository.OutboxEvent, error) {
	return nil, nil
}

func (s *fakeStore) CreateOutboxEvent(_ context.Context, arg repository.CreateOutboxEventParams) (int64, error) {
	s.insertAttempts++
	if s.failNext != nil {
		err := s.failNext
		s.failNext = nil
		return 0, err
	}
	if _, exists := s.rows[arg.ID]; exists {
		return 0, nil
	}
	s.rows[arg.ID] = arg
	return 1, nil
}

func (*fakeStore) DeletePublishedOutboxEvents(context.Context) (int64, error) { return 0, nil }

func (*fakeStore) MarkOutboxEventFailed(context.Context, repository.MarkOutboxEventFailedParams) error {
	return nil
}

func (*fakeStore) MarkOutboxEventPublished(context.Context, uuid.UUID) error { return nil }

func (*fakeStore) ReleaseOutboxEventLock(context.Context, uuid.UUID) error { return nil }

var _ repository.Store = (*fakeStore)(nil)
