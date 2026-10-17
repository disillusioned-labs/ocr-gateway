-- The routed outbox row uses a deterministic ID derived from the source
-- event_id. Its primary key is the durable dedupe guard on redelivery.

-- name: CreateOutboxEvent :execrows
INSERT INTO outbox_events (aggregate_type,
                           aggregate_id,
                           id,
                           event_type,
                           event_version,
                           topic,
                           payload,
                           trace_id)
VALUES ($1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8)
ON CONFLICT (id) DO NOTHING;

-- name: ClaimPendingOutboxEvents :many
WITH pending_events AS (SELECT id
                        FROM outbox_events
                        WHERE published_at IS NULL
                          AND (
                            next_attempt_at IS NULL
                                OR next_attempt_at <= NOW()
                            )
                          AND (
                            locked_at IS NULL
                                OR locked_at < NOW() - INTERVAL '5 minutes'
                            )
                        ORDER BY created_at ASC, id ASC
    LIMIT $1
    FOR
UPDATE SKIP LOCKED
    )
UPDATE outbox_events AS outbox
SET locked_at = NOW(),
    locked_by = $2 FROM pending_events
WHERE outbox.id = pending_events.id
    RETURNING
    outbox.id
    , outbox.aggregate_type
    , outbox.aggregate_id
    , outbox.event_type
    , outbox.event_version
    , outbox.topic
    , outbox.payload
    , outbox.created_at
    , outbox.published_at
    , outbox.trace_id
    , outbox.attempt_count
    , outbox.next_attempt_at
    , outbox.locked_at
    , outbox.locked_by
    , outbox.last_error;

-- name: MarkOutboxEventPublished :exec
UPDATE outbox_events
SET published_at = NOW(),
    locked_at    = NULL,
    locked_by    = NULL,
    last_error   = NULL
WHERE id = $1
  AND published_at IS NULL;

-- name: MarkOutboxEventFailed :exec
UPDATE outbox_events
SET attempt_count   = attempt_count + 1,
    next_attempt_at = $2,
    last_error      = $3,
    locked_at       = NULL,
    locked_by       = NULL
WHERE id = $1
  AND published_at IS NULL;

-- name: ReleaseOutboxEventLock :exec
UPDATE outbox_events
SET locked_at = NULL,
    locked_by = NULL
WHERE id = $1
  AND published_at IS NULL;

-- name: DeletePublishedOutboxEvents :execrows
DELETE
FROM outbox_events
WHERE published_at IS NOT NULL
  AND published_at < NOW() - INTERVAL '7 days';
