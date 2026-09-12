package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/disillusioned-labs/ocr-gateway/internal/config"
	"github.com/disillusioned-labs/ocr-gateway/internal/handler/health"
	"github.com/disillusioned-labs/ocr-gateway/internal/server"
	platformconfig "github.com/disillusioned-labs/platform/config"
	"github.com/disillusioned-labs/platform/postgres"
	"github.com/disillusioned-labs/platform/redis"
	"github.com/disillusioned-labs/platform/telemetry"

	migrations "github.com/disillusioned-labs/ocr-gateway/db/migrations"

	goredis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

// otelFlushTimeout bounds the trace flush at exit: if the OTLP collector is
// unreachable, the batch exporter blocks indefinitely and the process never
// exits.
const otelFlushTimeout = 5 * time.Second

// RunGRPC boots the Kontrak A surface (DocumentGateway) with the probes
// listener next to it, and blocks until the process is told to stop. The
// caller owns loading and validating cfg (see cmd/grpc).
func RunGRPC(cfg *config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := telemetry.NewLogger(cfg.Log.Level,
		telemetry.Format(cfg.Log.Format),
		telemetry.Env(cfg.Service.Env),
		telemetry.Service(cfg.Service.Name),
	)
	slog.SetDefault(log)
	log.Info("starting", "service", cfg.Service.Name, "role", "grpc", "build", buildInfo())

	// -------------------------------------------------------------------------
	// Telemetry
	// -------------------------------------------------------------------------
	otelOpts := []telemetry.Option{telemetry.WithBuild(version, commit)}
	if cfg.OTel.TracesEnabled() {
		sampler, err := telemetry.NewSampler(cfg.OTel.TracesSampler, cfg.OTel.TracesSamplerArg)
		if err != nil {
			return fmt.Errorf("configure trace sampler: %w", err)
		}
		otelOpts = append(otelOpts, telemetry.WithTracing(cfg.OTel.TraceEndpoint(), sampler))
	}
	if cfg.OTel.MetricsEnabled() {
		otelOpts = append(otelOpts, telemetry.WithMetrics(
			cfg.OTel.MetricEndpoint(), cfg.OTel.MetricExportInterval(),
		))
	}
	shutdownOtel, err := telemetry.Setup(ctx, cfg.Service.Name, cfg.Service.Env, otelOpts...)
	if err != nil {
		return fmt.Errorf("setup telemetry: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), otelFlushTimeout)
		defer cancel()
		if err := shutdownOtel(flushCtx); err != nil {
			log.Error("otel shutdown failed", "error", err)
		}
	}()

	// -------------------------------------------------------------------------
	// PostgreSQL
	// -------------------------------------------------------------------------
	pool, err := postgres.NewPool(ctx, cfg.Postgres.DSN,
		postgres.MaxConns(cfg.Postgres.MaxConns),
		postgres.MinConns(cfg.Postgres.MinConns),
		postgres.MaxConnLifetime(cfg.Postgres.MaxConnLifetime),
		postgres.QueryExecMode(cfg.Postgres.QueryExecMode),
	)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	log.Info("connected to postgres", "postgres", cfg.Postgres)

	if cfg.Postgres.Migrate {
		if err := postgres.Migrate(ctx, pool, migrations.FS, log); err != nil {
			return fmt.Errorf("run migrations: %w", err)
		}
	}

	// -------------------------------------------------------------------------
	// Redis (dedupe + rate limit live here, so required mode is the default)
	// -------------------------------------------------------------------------
	rdb, closeRedis, err := setupRedis(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer closeRedis()

	// -------------------------------------------------------------------------
	// Dependencies
	// -------------------------------------------------------------------------
	ocrClient, closeOcr, err := newOcrClient(ctx, cfg, log)
	if err != nil {
		return fmt.Errorf("connect ocr gRPC: %w", err)
	}
	defer closeOcr()

	svc, err := buildDeps(cfg, ocrClient, log)
	if err != nil {
		return fmt.Errorf("build dependencies: %w", err)
	}

	// -------------------------------------------------------------------------
	// Servers
	// -------------------------------------------------------------------------
	grpcSrv, err := server.NewGRPC(cfg, log, rdb, svc)
	if err != nil {
		return fmt.Errorf("create grpc server: %w", err)
	}

	// Readyz pings postgres and redis; the engine is deliberately NOT probed:
	// ocr being down is UNAVAILABLE to callers, not a gateway readiness
	// failure - pulling the gateway from rotation would not heal the engine.
	httpSrv := server.NewHTTPServer(cfg.Server.Port, map[string]health.Pinger{
		"postgres": pool,
		"redis":    redisPinger(rdb),
	}, nil, log)

	pprofSrv := server.NewPprofServer(cfg.Pprof.Enabled, cfg.Pprof.Port, log)

	// -------------------------------------------------------------------------
	// Serve
	// -------------------------------------------------------------------------
	g, runCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := grpcSrv.Start(); err != nil {
			return fmt.Errorf("grpc listener: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := httpSrv.Start(); err != nil {
			return fmt.Errorf("probe listener: %w", err)
		}
		return nil
	})
	if pprofSrv != nil {
		g.Go(func() error {
			if err := pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("pprof listener: %w", err)
			}
			return nil
		})
	}

	<-runCtx.Done()
	log.Info("shutdown initiated", "cause", shutdownCause(ctx.Err() != nil))
	stop()

	// Fail readiness first, then drain: Kubernetes removes endpoints
	// asynchronously, and closing the gRPC listener the instant SIGTERM lands
	// is the usual source of 502s during a rolling deploy.
	httpSrv.BeginDrain()
	grpcSrv.BeginDrain()
	if d := cfg.Server.DrainDelay; d > 0 {
		log.Info("draining: failing readiness before shutdown", "drain_delay", d)
		time.Sleep(d)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	if pprofSrv != nil {
		if err := pprofSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("pprof shutdown: %w", err)
		}
	}
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("probe server shutdown: %w", err)
	}
	if err := grpcSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("grpc graceful shutdown: %w", err)
	}
	if err := g.Wait(); err != nil {
		return err
	}

	log.Info("shutdown complete")
	return nil
}

// setupRedis connects per redis.mode; the gateway never runs truly without
// Redis (config rejects disabled), so optional mode degrades only for as long
// as the connection is down.
func setupRedis(ctx context.Context, cfg *config.Config, log *slog.Logger) (*goredis.Client, func(), error) {
	noop := func() {}

	client, err := redis.New(ctx, cfg.Redis.Addr,
		redis.Password(cfg.Redis.Password),
		redis.DB(cfg.Redis.DB),
	)
	if err != nil {
		if cfg.Redis.Mode == platformconfig.RedisModeRequired {
			return nil, noop, fmt.Errorf("connect redis (required): %w", err)
		}
		// Optional: the rate limiter fails open and the router fails closed
		// on Redis errors, so boot continues with a loud warning.
		log.Warn("redis unreachable; dedupe and rate limiting degraded", "error", err)
		return nil, noop, nil
	}

	log.Info("connected to redis", "redis", cfg.Redis)
	return client, func() { _ = client.Close() }, nil
}

// redisPinger adapts the Redis client to a probe; in optional mode the client
// may be nil, and then the probe reports the degradation instead of panicking.
func redisPinger(rdb *goredis.Client) health.Pinger {
	if rdb == nil {
		return server.PingFunc(func(ctx context.Context) error {
			return errors.New("redis unavailable (optional mode)")
		})
	}
	return server.PingFunc(func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	})
}

func shutdownCause(signalled bool) string {
	if signalled {
		return "signal"
	}
	return "listener failure"
}
