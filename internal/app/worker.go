package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/disillusioned-labs/ocr-gateway/internal/config"
	"github.com/disillusioned-labs/ocr-gateway/internal/repository"
	"github.com/disillusioned-labs/ocr-gateway/internal/service/outbox"
	"github.com/disillusioned-labs/platform/kafka"
	"github.com/disillusioned-labs/platform/postgres"
	"github.com/disillusioned-labs/platform/telemetry"

	migrations "github.com/disillusioned-labs/ocr-gateway/db/migrations"

	"golang.org/x/sync/errgroup"
)

const (
	publishInterval = 1 * time.Second
	workerIDPrefix  = "outbox-worker-"
)

// RunWorker is the outbox publisher: it drains the gateway's routed events to
// Kafka. It is producer-only; consuming lives in RunConsumer
// (internal/app/consumer.go) so the two roles scale and restart independently.
func RunWorker(cfg *config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := telemetry.NewLogger(cfg.Log.Level,
		telemetry.Format(cfg.Log.Format),
		telemetry.Env(cfg.Service.Env),
		telemetry.Service(cfg.Service.Name),
	)
	slog.SetDefault(log)
	log.Info("starting", "service", cfg.Service.Name, "role", "worker", "build", buildInfo())

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
	})
	if err != nil {
		return fmt.Errorf("connect kafka: %w", err)
	}
	defer kafkaClient.Close()
	producer := kafka.NewProducer(kafkaClient)

	publisher := outbox.NewOutboxService(repo, producer, log)
	workerID := workerIDPrefix + cfg.Service.InstanceID

	g, runCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		ticker := time.NewTicker(publishInterval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return nil
			case <-ticker.C:
				// A failed tick is logged, not fatal: the outbox keeps its
				// rows, the lock expires, and the next tick drains again.
				if err := publisher.PublishPending(runCtx, workerID, 100); err != nil {
					log.Error("outbox publish tick failed", "error", err)
				}
			}
		}
	})

	<-runCtx.Done()
	log.Info("shutdown initiated", "cause", shutdownCause(ctx.Err() != nil))
	if err := g.Wait(); err != nil {
		return err
	}
	log.Info("shutdown complete")
	return nil
}
