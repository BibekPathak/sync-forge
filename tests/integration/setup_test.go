//go:build integration

// Package integration contains end-to-end tests that require a live
// PostgreSQL instance. Run with: make test-integration
package integration

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"syncforge/internal/db"
	"syncforge/migrations"
)

var (
	appDSN   = getenv("DATABASE_URL", "postgres://syncforge_app:syncforge_app@localhost:5432/syncforge_test?sslmode=disable")
	adminDSN = getenv("ADMIN_DATABASE_URL", "postgres://syncforge_engine:syncforge_engine@localhost:5432/syncforge_test?sslmode=disable")
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// newDB connects and applies migrations idempotently, then wipes any leftover
// data from prior runs so tests are reproducible.
func newDB(t *testing.T) *db.DB {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	database, err := db.Connect(context.Background(), appDSN, adminDSN, logger)
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	t.Cleanup(database.Close)
	if err := db.Migrate(context.Background(), database.App, migrations.FS, logger); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := database.Admin.Exec(context.Background(),
		`TRUNCATE audit_log, outbound_writes, sync_operations, reconciliation_runs, sync_jobs,
		         conflicts, dead_letter, retry_queue, processed_events, source_events,
		         canonical_records, sync_policies, connections, api_keys, users, tenants RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("reset db: %v", err)
	}
	return database
}
