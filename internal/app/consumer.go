package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/disillusioned-labs/ocr-gateway/internal/config"
	"github.com/disillusioned-labs/ocr-gateway/internal/constant"
	"github.com/disillusioned-labs/ocr-gateway/internal/envelope"
	"github.com/disillusioned-labs/ocr-gateway/internal/repository"
	"github.com/disillusioned-labs/ocr-gateway/internal/server"
	"github.com/disillusioned-labs/ocr-gateway/internal/service/router"
	"github.com/disillusioned-labs/platform/kafka"
	"github.com/disillusioned-labs/platform/postgres"
	"github.com/disillusioned-labs/platform/telemetry"

	migrations "github.com/disillusioned-labs/ocr-gateway/db/migrations"
)

// RunConsumer is the document.processed consumer: dedupe by event_id, route
// by caller_id, and make the routed event durable in the gateway's own outbox
// before committing the offset. It is its own binary so it scales and
// restarts independently of the publisher.
func RunConsumer(cfg *config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := telemetry.NewLogger(cfg.Log.Level,
		telemetry.Format(cfg.Log.Format),
		telemetry.Env(cfg.Service.Env),
		telemetry.Service(cfg.Service.Name),
	)
	slog.SetDefault(log)
	log.Info("starting", "service", cfg.Service.Name, "role", "consumer", "build", buildInfo())

	// Unlike the worker, a consumer with no group configured is a
	// misdeployment, not a no-op: the whole point of this binary is to consume.
	if cfg.Kafka.Consumer.Group == "" {
		return fmt.Errorf("consumer requires KAFKA_CONSUMER_GROUP and KAFKA_CONSUMER_TOPICS to be set")
	}

	// The dedupe mark lives in Redis; consuming without it would turn every
	// redelivery into a second routed event, so this binary has no degraded
	// mode even when the deployment allows redis.mode=optional.
	rdb, closeRedis, err := setupRedis(ctx, cfg, log)
	if err != nil {
		return err
	}
	if rdb == nil {
		return fmt.Errorf("consumer requires a reachable redis (dedupe); set redis.mode=required or fix the connection")
	}
	defer closeRedis()

	pool, err := postgres.NewPool(ctx, cfg.Postgres.DSN,
		postgres.MaxConns(cfg.Postgres.MaxConns),
		postgres.MinConns(1),
	)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	if cfg.Postgres.Migrate {
		if err := postgres.Migrate(ctx, pool, migrations.FS, log); err != nil {
			return fmt.Errorf("run migrations: %w", err)
		}
	}
	repo := repository.NewStore(pool)

	kafkaClient, err := kafka.New(ctx, kafka.KafkaConfig{
		Brokers:     cfg.Kafka.Brokers,
		ClientID:    cfg.Kafka.ClientID,
		PingTimeout: cfg.Kafka.PingTimeout,
		Producer: kafka.ProducerConfig{
			RecordRetries:         cfg.Kafka.Producer.RecordRetries,
			RecordDeliveryTimeout: cfg.Kafka.Producer.RecordDeliveryTimeout,
		},
		Consumer: kafka.ConsumerConfig{
			Group:    cfg.Kafka.Consumer.Group,
			Topics:   cfg.Kafka.Consumer.Topics,
			DLQTopic: cfg.Kafka.Consumer.DLQTopic,
			Retry: kafka.RetryConfig{
				MaxAttempts:  cfg.Kafka.Consumer.Retry.MaxAttempts,
				InitialDelay: cfg.Kafka.Consumer.Retry.InitialDelay,
				MaxDelay:     cfg.Kafka.Consumer.Retry.MaxDelay,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("connect kafka: %w", err)
	}
	defer kafkaClient.Close()

	producer := kafka.NewProducer(kafkaClient)
	consumer := kafka.NewConsumer(kafkaClient)

	log.Info("document.processed consumer listening",
		"group", cfg.Kafka.Consumer.Group,
		"topics", cfg.Kafka.Consumer.Topics,
		"dlq_topic", cfg.Kafka.Consumer.DLQTopic,
	)

	registry := server.NewCallerRegistry(cfg.Gateway.Callers)
	routerSvc := router.NewService(repo, rdb, router.Options{
		DedupeTTL:           cfg.Gateway.DedupeTTL,
		RoutedTopicTemplate: cfg.Gateway.RoutedTopicTemplate,
		KnownCaller:         registry.KnownCaller,
	}, log)
	dlq := kafka.NewDLQPublisher(producer, cfg.Kafka.Consumer.DLQTopic, log)

	for {
		records, err := consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				commitPending(consumer, log)
				return nil
			}
			return fmt.Errorf("poll %s: %w", constant.TopicDocumentProcessed, err)
		}
		for _, rec := range records {
			if err := handleProcessedRecord(ctx, routerSvc, dlq, rec, log); err != nil {
				// Transient failure (redis, postgres): the offset is NOT
				// committed, so Kafka redelivers. Committing would silently
				// drop the event.
				log.Error("document.processed record failed; will be redelivered", "error", err,
					"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset)
				commitPending(consumer, log)
				return fmt.Errorf("route document.processed record: %w", err)
			}
			if err := consumer.CommitRecords(ctx, rec); err != nil {
				commitPending(consumer, log)
				return fmt.Errorf("commit %s offset: %w", constant.TopicDocumentProcessed, err)
			}
		}
	}
}

// commitPending flushes any processed-but-uncommitted offsets to the broker.
// It uses a fresh context because the caller's context is already cancelled.
func commitPending(consumer *kafka.Consumer, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := consumer.CommitUncommitted(ctx); err != nil {
		log.Error("failed to commit pending offsets during shutdown", "error", err)
	}
}

func handleProcessedRecord(
	ctx context.Context,
	routerSvc router.Service,
	dlq *kafka.DLQPublisher,
	rec kafka.Record,
	log *slog.Logger,
) error {
	processCtx := ctx
	if rec.Context != nil {
		var cancel context.CancelFunc
		processCtx, cancel = context.WithCancel(rec.Context)
		defer cancel()
		go func() {
			<-ctx.Done()
			cancel()
		}()
	}

	env, err := envelope.Parse(rec.Value)
	if err != nil {
		// A record that violates the contract can never succeed; dead-letter
		// it and commit so one poison payload does not stall the partition.
		return deadLetter(ctx, dlq, rec, err, log)
	}
	// Verify the header against the envelope (routing.md: "verifikasi header +
	// baca event"): event-id is the dedupe key, so a mismatch between the two
	// carriers of it is a contract violation, not a routing problem.
	if hdrEventID := recordHeader(rec, constant.HeaderEventID); hdrEventID != "" && hdrEventID != env.EventID {
		return deadLetter(ctx, dlq, rec,
			fmt.Errorf("header event-id %q does not match envelope event_id %q", hdrEventID, env.EventID), log)
	}

	traceID := recordHeader(rec, constant.HeaderTraceID)
	if err := routerSvc.Handle(processCtx, env, traceID); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(processCtx.Err(), context.Canceled) {
			return err
		}
		var permanent *router.PermanentError
		if errors.As(err, &permanent) {
			return deadLetter(ctx, dlq, rec, err, log)
		}
		return err
	}
	return nil
}

// recordHeader returns the first value of the named header, or "".
func recordHeader(rec kafka.Record, name string) string {
	for _, h := range rec.Headers {
		if h.Key == name {
			return string(h.Value)
		}
	}
	return ""
}

func deadLetter(ctx context.Context, dlq *kafka.DLQPublisher, rec kafka.Record, cause error, log *slog.Logger) error {
	err := dlq.Publish(ctx, rec, kafka.DLQMetadata{
		SourceTopic:     rec.Topic,
		SourcePartition: rec.Partition,
		SourceOffset:    rec.Offset,
		Attempt:         1,
		ErrorType:       "ocr_router_error",
		ErrorCode:       "OCR_ROUTER_ERROR",
	})
	if err != nil {
		log.Error("dead-letter publish failed; record will be redelivered",
			"error", err, "cause", cause, "topic", rec.Topic, "offset", rec.Offset)
		return err
	}
	log.Warn("record dead-lettered", "cause", cause, "topic", rec.Topic, "offset", rec.Offset)
	return nil
}
