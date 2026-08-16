package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds runtime configuration for all SyncForge services.
// Values are read from environment variables with sensible local defaults.
type Config struct {
	Env        string
	Service    string
	HTTPAddr   string
	DBURL      string
	AdminDBURL string
	RedisAddr  string

	KafkaBrokers string
	KafkaGroupID string

	// OTLPEndpoint enables OpenTelemetry trace export (e.g.
	// "collector:4318"). Empty disables tracing (no-op provider).
	OTLPEndpoint string

	SeedAcme       bool
	SeedSFBaseURL  string
	SeedHubBaseURL string
	SeedSFSSecret  string
	SeedHubSecret  string

	RetryBaseDelayMs int
	RetryMaxDelayMs  int
	RetryMaxAttempts int
	SyncJobBatchSize int

	ShutdownTimeout time.Duration
}

func Load() Config {
	return Config{
		Env:        get("SYNCFORGE_ENV", "development"),
		Service:    get("SYNCFORGE_SERVICE", "api"),
		HTTPAddr:   get("HTTP_ADDR", ":8080"),
		DBURL:      get("DATABASE_URL", "postgres://syncforge_app:syncforge_app@localhost:5432/syncforge?sslmode=disable"),
		AdminDBURL: get("ADMIN_DATABASE_URL", "postgres://syncforge_engine:syncforge_engine@localhost:5432/syncforge?sslmode=disable"),
		RedisAddr:  get("REDIS_ADDR", "localhost:6379"),

		KafkaBrokers: get("KAFKA_BROKERS", "localhost:29092"),
		KafkaGroupID: get("KAFKA_GROUP_ID", "syncforge-engine"),
		OTLPEndpoint: get("OTEL_EXPORTER_OTLP_ENDPOINT", ""),

		SeedAcme:       getBool("SYNCFORGE_SEED_ACME", true),
		SeedSFBaseURL:  get("SYNCFORGE_SEED_SALESFORCE_URL", "http://localhost:9081"),
		SeedHubBaseURL: get("SYNCFORGE_SEED_HUBSPOT_URL", "http://localhost:9082"),
		SeedSFSSecret:  get("SYNCFORGE_SEED_SALESFORCE_WEBHOOK_SECRET", "sfs-dev-secret"),
		SeedHubSecret:  get("SYNCFORGE_SEED_HUBSPOT_WEBHOOK_SECRET", "sfh-dev-secret"),

		RetryBaseDelayMs: getInt("SYNCFORGE_RETRY_BASE_DELAY_MS", 1000),
		RetryMaxDelayMs:  getInt("SYNCFORGE_RETRY_MAX_DELAY_MS", 60000),
		RetryMaxAttempts: getInt("SYNCFORGE_RETRY_MAX_ATTEMPTS", 8),
		SyncJobBatchSize: getInt("SYNCFORGE_SYNC_JOB_BATCH_SIZE", 1000),

		ShutdownTimeout: time.Duration(getInt("SYNCFORGE_SHUTDOWN_TIMEOUT_MS", 10000)) * time.Millisecond,
	}
}

func get(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getBool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func getInt(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func (c Config) String() string {
	return fmt.Sprintf("service=%s env=%s addr=%s", c.Service, c.Env, c.HTTPAddr)
}
