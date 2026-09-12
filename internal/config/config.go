// Package config loads and validates application configuration from environment
// variables (e.g. SERVER_PORT=8083), with a local .env file as a development
// convenience and production-safe defaults underneath.
//
// Variable names are unprefixed, sharing a namespace with everything else in
// the environment. "service" is named defensively so it does not collide with
// generic NAME/ENV; "otel" deliberately does the opposite and adopts the
// OpenTelemetry SDK's own spelling (see OTelConfig). Check any new key against
// what an orchestrator might already set (Kubernetes injects <SERVICE>_PORT
// for every Service in the namespace).
//
// .env.example is the single list of available settings, written exactly as
// a deployment sets them - there is deliberately one vocabulary, not a
// config.yaml shadowing the env vars.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/viper"

	platformconfig "github.com/disillusioned-labs/platform/config"
)

// Config is the root of all application settings, one field per subsystem.
type Config struct {
	Service    platformconfig.ServiceConfig    `mapstructure:"service"`
	Server     platformconfig.ServerConfig     `mapstructure:"server"`
	Pprof      platformconfig.PprofConfig      `mapstructure:"pprof"`
	Postgres   platformconfig.PostgresConfig   `mapstructure:"postgres"`
	Redis      platformconfig.RedisConfig      `mapstructure:"redis"`
	Kafka      platformconfig.KafkaConfig      `mapstructure:"kafka"`
	OTel       platformconfig.OTelConfig       `mapstructure:"otel"`
	Log        platformconfig.LogConfig        `mapstructure:"log"`
	GRPC       platformconfig.GRPCConfig       `mapstructure:"grpc"`
	GRPCClient platformconfig.GRPCClientConfig `mapstructure:"grpc_client"`
	Ocr        OcrClientConfig                 `mapstructure:"ocr"`
	Gateway    GatewayConfig                   `mapstructure:"gateway"`
}

// OcrClientConfig names where the ocr service's gRPC surface lives (Kontrak
// B). Targets are per-dependency keys, kept out of GRPCClientConfig (shared
// outbound knobs) so the EnvKey-based .env layering names each variable after
// who it dials.
type OcrClientConfig struct {
	// GRPCTarget is the ocr service's gRPC address (document.v1). Must not be
	// empty: forwarding submits to the engine is the gateway's entire job.
	GRPCTarget string `mapstructure:"grpc_target"`
}

// GatewayConfig is the ocr-gateway-specific section: the caller registry, the
// event-dedupe window, and the routed-topic naming rule.
type GatewayConfig struct {
	// DedupeTTL is the Redis TTL of the event-id dedupe key. The docs floor
	// it at 24 hours: redeliveries can arrive at least that long after the
	// first delivery (at-least-once), and a shorter TTL would let a late
	// duplicate through as a second routed event.
	DedupeTTL time.Duration `mapstructure:"dedupe_ttl"`
	// RoutedTopicTemplate is the routed-topic naming rule, exactly one %s
	// filled with the caller_id (e.g. ocr.document.routed.%s.v1).
	RoutedTopicTemplate string `mapstructure:"routed_topic_template"`
	// Callers is the per-service caller registry, parsed from the
	// GATEWAY_CALLERS JSON (a list would otherwise need one env var per
	// caller, and adding a caller must never need a config change).
	Callers []CallerConfig `mapstructure:"-"`
}

// CallerConfig is one registered caller: its credential, its rate budget, and
// the doc_types it may submit with their schema_id mapping. Adding a caller =
// adding an entry here (plus the routed topic in Kafka); nothing else changes.
type CallerConfig struct {
	// Name is the caller_id echoed into Kontrak B and events. It becomes part
	// of the routed topic name, so it is constrained to topic-safe characters.
	Name string
	// APIKeyHash is the hex SHA-256 of the caller's API key. The raw key is
	// never stored in config; the interceptor hashes what was presented and
	// compares digests.
	APIKeyHash string
	// RateLimit bounds how many SubmitDocument calls this caller may make per
	// Window (Redis fixed window, keyed by caller).
	RateLimit RateLimitConfig
	// DocTypes maps the business name ("receipt") to the engine's schema_id
	// ("receipt@1"). Unknown doc_type is rejected at the door (INVALID_ARGUMENT).
	DocTypes map[string]string
}

// RateLimitConfig is one caller's submit budget.
type RateLimitConfig struct {
	Requests int
	Window   time.Duration
}

// DotEnvFile is the optional local overrides file, loaded from the working
// directory. It is git-ignored; .env.example documents every key.
const DotEnvFile = ".env"

// callersVar is the JSON-encoded caller registry.
const callersVar = "GATEWAY_CALLERS"

// callerNamePattern is what a caller name may look like. It must survive as a
// Kafka topic segment (ocr.document.routed.<name>.v1), so no dots, no wilds.
var callerNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

// apiKeyHashPattern is a lowercase hex SHA-256 digest.
var apiKeyHashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Load builds the configuration from environment variables (e.g. POSTGRES_DSN),
// falling back to the defaults in setDefaults.
//
// A .env file in the working directory is loaded into the environment first as
// a local development convenience; real environment variables always win, so
// the precedence is environment > .env > defaults. Deployments set variables
// directly and ship no file.
func Load() (*Config, error) {
	dotEnv, err := platformconfig.ParseDotEnv(DotEnvFile)
	if err != nil {
		return nil, err
	}

	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	setDefaults(v)

	// Layer .env in as defaults rather than by setting process environment
	// variables. Viper resolves AutomaticEnv before defaults, so a real
	// environment variable still wins for free - and Load leaves no global
	// state behind, which keeps it idempotent and safe to call from tests.
	for _, key := range v.AllKeys() {
		if value, ok := dotEnv[platformconfig.EnvKey(key)]; ok {
			v.SetDefault(key, value)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	cfg.Kafka.Brokers = platformconfig.NormalizeKafkaBrokers(cfg.Kafka.Brokers)
	cfg.Kafka.Consumer.Topics = platformconfig.NormalizeKafkaTopics(cfg.Kafka.Consumer.Topics)
	cfg.Service.InstanceID = platformconfig.InstanceID()

	callers, err := parseCallers(v.GetString("gateway.callers"))
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	cfg.Gateway.Callers = callers

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

// parseCallers decodes the GATEWAY_CALLERS JSON. Windows come in as strings
// because a raw JSON unmarshal would demand integer nanoseconds for a
// time.Duration - the same reason config paths parse durations via Viper's
// hook.
func parseCallers(raw string) ([]CallerConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	type rawCaller struct {
		Name       string `json:"name"`
		APIKeyHash string `json:"api_key_hash"`
		RateLimit  *struct {
			Requests int    `json:"requests"`
			Window   string `json:"window"`
		} `json:"rate_limit"`
		DocTypes map[string]string `json:"doc_types"`
	}
	var raws []rawCaller
	if err := json.Unmarshal([]byte(raw), &raws); err != nil {
		return nil, fmt.Errorf("parse %s as JSON list: %w", callersVar, err)
	}

	callers := make([]CallerConfig, 0, len(raws))
	for _, rc := range raws {
		c := CallerConfig{
			Name:       rc.Name,
			APIKeyHash: strings.ToLower(rc.APIKeyHash),
			DocTypes:   rc.DocTypes,
		}
		if rc.RateLimit != nil {
			window, err := time.ParseDuration(rc.RateLimit.Window)
			if err != nil {
				return nil, fmt.Errorf("caller %q: rate_limit.window: %w", rc.Name, err)
			}
			c.RateLimit = RateLimitConfig{Requests: rc.RateLimit.Requests, Window: window}
		}
		callers = append(callers, c)
	}
	return callers, nil
}

// validate rejects every value the app would otherwise silently misinterpret.
// A boilerplate is copied far more often than it is read, so an unset or
// fat-fingered override must fail at boot rather than degrade in production.
func (c *Config) validate() error {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if err := platformconfig.ValidateService(&c.Service); err != nil {
		errs = append(errs, err)
	}

	if c.Server.Port < 1 || c.Server.Port > 65535 {
		fail("server.port must be in 1..65535, got %d", c.Server.Port)
	}
	if c.Pprof.Enabled {
		if c.Pprof.Port < 1 || c.Pprof.Port > 65535 {
			fail("pprof.port must be in 1..65535 when pprof.enabled, got %d", c.Pprof.Port)
		}
		if c.Pprof.Port == c.Server.Port {
			fail("pprof.port (%d) must differ from server.port", c.Pprof.Port)
		}
	}
	for _, d := range []struct {
		key string
		val time.Duration
	}{
		{"server.read_timeout", c.Server.ReadTimeout},
		{"server.write_timeout", c.Server.WriteTimeout},
		{"server.idle_timeout", c.Server.IdleTimeout},
		{"server.shutdown_timeout", c.Server.ShutdownTimeout},
	} {
		if d.val <= 0 {
			fail("%s must be > 0, got %s", d.key, d.val)
		}
	}
	if c.Server.DrainDelay < 0 {
		fail("server.drain_delay must not be negative, got %s", c.Server.DrainDelay)
	}

	if err := platformconfig.ValidatePostgres(&c.Postgres); err != nil {
		errs = append(errs, err)
	}

	// Redis validation. The gateway's dedupe and per-caller rate limit both
	// live in Redis, so required is the honest default - a "degraded" gateway
	// would either double-route events or let submit bursts through.
	switch c.Redis.Mode {
	case platformconfig.RedisModeDisabled:
		fail("redis.mode must not be disabled: dedupe and rate limiting need redis")
	case platformconfig.RedisModeOptional, platformconfig.RedisModeRequired:
		if c.Redis.Addr == "" {
			fail("redis.addr must be set when redis.mode is %s", c.Redis.Mode)
		}
	default:
		fail("redis.mode must be one of optional|required, got %q", c.Redis.Mode)
	}
	if c.Redis.DB < 0 {
		fail("redis.db must not be negative, got %d", c.Redis.DB)
	}

	if err := platformconfig.ValidateKafka(&c.Kafka); err != nil {
		errs = append(errs, err)
	}
	if c.Kafka.Producer.RecordRetries < 0 {
		fail("kafka.producer.record_retries must be >= 0, got %d", c.Kafka.Producer.RecordRetries)
	}
	if c.Kafka.Producer.RecordDeliveryTimeout <= 0 {
		fail("kafka.producer.record_delivery_timeout must be > 0, got %s", c.Kafka.Producer.RecordDeliveryTimeout)
	}

	if err := platformconfig.ValidateOTel(&c.OTel); err != nil {
		errs = append(errs, err)
	}
	if err := platformconfig.ValidateLog(&c.Log); err != nil {
		errs = append(errs, err)
	}

	if err := platformconfig.ValidateGRPCClient(&c.GRPCClient); err != nil {
		errs = append(errs, err)
	}
	if strings.TrimSpace(c.Ocr.GRPCTarget) == "" {
		errs = append(errs, fmt.Errorf("ocr.grpc_target must not be empty"))
	}
	if err := platformconfig.ValidateGRPC(&c.GRPC); err != nil {
		errs = append(errs, err)
	}
	if c.GRPC.ServerPort == c.Server.Port {
		fail("grpc.server_port (%d) must differ from server.port", c.GRPC.ServerPort)
	}

	// Gateway validation.
	if c.Gateway.DedupeTTL < 24*time.Hour {
		errs = append(errs, fmt.Errorf("gateway.dedupe_ttl must be >= 24h, got %s (docs floor: redeliveries can arrive that late)", c.Gateway.DedupeTTL))
	}
	if c.Gateway.RoutedTopicTemplate == "" {
		errs = append(errs, errors.New("gateway.routed_topic_template must not be empty"))
	} else if strings.Count(c.Gateway.RoutedTopicTemplate, "%s") != 1 {
		fail("gateway.routed_topic_template must contain exactly one %%s, got %q", c.Gateway.RoutedTopicTemplate)
	}
	if len(c.Gateway.Callers) == 0 {
		errs = append(errs, errors.New("gateway.callers must list at least one caller (a gateway nobody may call routes nothing)"))
	}
	seen := make(map[string]bool, len(c.Gateway.Callers))
	for _, caller := range c.Gateway.Callers {
		if !callerNamePattern.MatchString(caller.Name) {
			fail("caller %q: name must match %s (it becomes the routed topic segment)", caller.Name, callerNamePattern)
			continue
		}
		if seen[caller.Name] {
			fail("caller %q: duplicate name", caller.Name)
		}
		seen[caller.Name] = true
		if !apiKeyHashPattern.MatchString(caller.APIKeyHash) {
			fail("caller %q: api_key_hash must be a lowercase hex sha-256 digest (64 chars)", caller.Name)
		}
		if caller.RateLimit.Requests <= 0 {
			fail("caller %q: rate_limit.requests must be > 0", caller.Name)
		}
		if caller.RateLimit.Window <= 0 {
			fail("caller %q: rate_limit.window must be > 0", caller.Name)
		}
		if len(caller.DocTypes) == 0 {
			fail("caller %q: doc_types must map at least one doc_type to a schema_id", caller.Name)
		}
		for docType, schemaID := range caller.DocTypes {
			if docType == "" || schemaID == "" {
				fail("caller %q: doc_types entries must be non-empty (got %q -> %q)", caller.Name, docType, schemaID)
			}
		}
	}

	return errors.Join(errs...)
}

// setDefaults registers every key with its production-safe value; Viper's
// AutomaticEnv only resolves keys it already knows, so an unregistered key
// would be invisible even when its variable is set.
func setDefaults(v *viper.Viper) {
	v.SetDefault("service.name", "ocr-gateway")
	v.SetDefault("service.env", platformconfig.EnvDevelopment)

	// HTTP surface of the grpc binary: probes only (/healthz, /readyz). There
	// is no public REST API - the contract is Kontrak A over gRPC.
	v.SetDefault("server.port", 8083)
	v.SetDefault("server.read_timeout", "10s")
	v.SetDefault("server.write_timeout", "30s")
	v.SetDefault("server.idle_timeout", "60s")
	v.SetDefault("server.shutdown_timeout", "20s")
	v.SetDefault("server.drain_delay", "5s")
	// Off by default; the port is pre-filled so enabling it needs one variable.
	v.SetDefault("pprof.enabled", false)
	v.SetDefault("pprof.port", 6063)

	v.SetDefault("postgres.dsn", "postgres://ocr_gateway_app:devpassword@localhost:5432/ocr_gateway?sslmode=disable")
	v.SetDefault("postgres.max_conns", 10)
	v.SetDefault("postgres.min_conns", 2)
	v.SetDefault("postgres.max_conn_lifetime", "1h")
	v.SetDefault("postgres.migrate", false)
	v.SetDefault("postgres.query_exec_mode", "cache_statement")

	// Required, not optional: without Redis there is no dedupe and no rate
	// limit, and a gateway without those is a straight pipe to the engine.
	v.SetDefault("redis.mode", string(platformconfig.RedisModeRequired))
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)

	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.client_id", "ocr-gateway")
	v.SetDefault("kafka.ping_timeout", "5s")
	v.SetDefault("kafka.producer.record_retries", int64(5))
	v.SetDefault("kafka.producer.record_delivery_timeout", "30s")
	v.SetDefault("kafka.consumer.group", "")
	v.SetDefault("kafka.consumer.topics", "")
	v.SetDefault("kafka.consumer.dlq_topic", "")
	v.SetDefault("kafka.consumer.retry.max_attempts", 3)
	v.SetDefault("kafka.consumer.retry.initial_delay", "200ms")
	v.SetDefault("kafka.consumer.retry.max_delay", "5s")

	v.SetDefault("otel.sdk_disabled", false)
	v.SetDefault("otel.traces_exporter", platformconfig.OTelExporterOTLP)
	v.SetDefault("otel.metrics_exporter", platformconfig.OTelExporterOTLP)
	v.SetDefault("otel.exporter_otlp_endpoint", "http://localhost:4317")
	// Empty means "inherit the base endpoint"; registered anyway because
	// AutomaticEnv only resolves keys Viper already knows.
	v.SetDefault("otel.exporter_otlp_traces_endpoint", "")
	v.SetDefault("otel.exporter_otlp_metrics_endpoint", "")
	v.SetDefault("otel.traces_sampler", "parentbased_traceidratio")
	v.SetDefault("otel.traces_sampler_arg", 1.0)
	// Milliseconds, per spec - this is the OTel SDK's own default.
	v.SetDefault("otel.metric_export_interval", 60000)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")

	// gRPC server (Kontrak A, DocumentGateway). 9093 keeps it clear of
	// identity's gRPC (9090), expense's (9091), and Kafka (9092).
	v.SetDefault("grpc.server_port", 9093)
	v.SetDefault("grpc.max_recv_msg_size", 8*1024*1024)
	v.SetDefault("grpc.max_send_msg_size", 4*1024*1024)
	v.SetDefault("grpc.max_header_size", 8*1024)
	v.SetDefault("grpc.unary_timeout", "15s")
	v.SetDefault("grpc.tls.enabled", false)
	v.SetDefault("grpc.tls.ca_file", "")
	v.SetDefault("grpc.tls.cert_file", "")
	v.SetDefault("grpc.tls.key_file", "")
	v.SetDefault("grpc.tls.server_name", "")
	v.SetDefault("grpc.tls.mutual_tls", false)

	// gRPC client (ocr-gateway -> ocr engine, Kontrak B). 9094 is a
	// placeholder for the engine's port until repo ocr picks its own.
	v.SetDefault("ocr.grpc_target", "localhost:9094")
	v.SetDefault("grpc_client.timeout", "10s")
	v.SetDefault("grpc_client.max_recv_msg_size", 8*1024*1024)
	v.SetDefault("grpc_client.max_send_msg_size", 4*1024*1024)
	v.SetDefault("grpc_client.tls.enabled", false)
	v.SetDefault("grpc_client.tls.ca_file", "")
	v.SetDefault("grpc_client.tls.cert_file", "")
	v.SetDefault("grpc_client.tls.key_file", "")
	v.SetDefault("grpc_client.tls.server_name", "")
	v.SetDefault("grpc_client.tls.mutual_tls", false)

	v.SetDefault("gateway.dedupe_ttl", "24h")
	v.SetDefault("gateway.routed_topic_template", "ocr.document.routed.%s.v1")
	// The registry itself has no default: an empty gateway must fail at boot.
	// GATEWAY_CALLERS is parsed in Load, not here, because a list of callers
	// has no shape Viper can unmarshal (see parseCallers).
	v.SetDefault("gateway.callers", "")
}
