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

	BootstrapKey string

	KafkaBrokers string
	KafkaGroupID string

	SeedAcme       bool
	SeedSFBaseURL  string
	SeedHubBaseURL string
	SeedSFSSecret  string
	SeedHubSecret  string

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

		BootstrapKey: get("SYNCFORGE_BOOTSTRAP_KEY", "syncforge-admin-dev"),

		KafkaBrokers: get("KAFKA_BROKERS", "localhost:29092"),
		KafkaGroupID: get("KAFKA_GROUP_ID", "syncforge-engine"),

		SeedAcme:       getBool("SYNCFORGE_SEED_ACME", true),
		SeedSFBaseURL:  get("SYNCFORGE_SEED_SALESFORCE_URL", "http://localhost:9081"),
		SeedHubBaseURL: get("SYNCFORGE_SEED_HUBSPOT_URL", "http://localhost:9082"),
		SeedSFSSecret:  get("SYNCFORGE_SEED_SALESFORCE_WEBHOOK_SECRET", "sfs-dev-secret"),
		SeedHubSecret:  get("SYNCFORGE_SEED_HUBSPOT_WEBHOOK_SECRET", "sfh-dev-secret"),

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
