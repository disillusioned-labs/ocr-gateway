-- The outbox queries are the standard cross-service set (same shape as
-- expense's): the router inserts inside its single transaction, the worker
-- claims with SKIP LOCKED and marks published or failed.

-- name: CreateOutboxEvent :one
INSERT INTO outbox_events (aggregate_type,
                           aggregate_id,
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
        $7) RETURNING
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    event_version,
    topic,
    payload,
    created_at,
    published_at,
    trace_id,
    attempt_count,
    next_attempt_at,
    locked_at,
    locked_by,
    last_error;

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
