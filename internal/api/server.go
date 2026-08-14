package api

import (
	"context"
	"log/slog"

	"syncforge/internal/cache"
	"syncforge/internal/config"
	"syncforge/internal/db"
	"syncforge/internal/observability"
)

// Server is the SyncForge API process: REST control plane + webhook gateway.
type Server struct {
	cfg     config.Config
	db      *db.DB
	cache   *cache.Cache
	metrics *observability.ServiceMetrics
	log     *slog.Logger
}

func New(cfg config.Config, database *db.DB, c *cache.Cache, metrics *observability.ServiceMetrics, log *slog.Logger) *Server {
	return &Server{cfg: cfg, db: database, cache: c, metrics: metrics, log: log}
}

// DB exposes the underlying database handle (used by tests).
func (s *Server) DB() *db.DB { return s.db }

// SeedDemoTenant idempotently creates the demo "Acme" tenant, its Salesforce
// and HubSpot connections, and a known API key. This makes `docker compose up`
// immediately usable.
func (s *Server) SeedDemoTenant(ctx context.Context) error {
	_, err := s.seed(ctx)
	return err
}
