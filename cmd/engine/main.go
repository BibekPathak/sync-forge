package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"syncforge/internal/config"
	"syncforge/internal/db"
	"syncforge/internal/observability"
	"syncforge/migrations"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := config.Load()
	cfg.Service = "engine"
	logger.Info("starting engine", "config", cfg.String())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	database, err := db.Connect(ctx, cfg.DBURL, cfg.AdminDBURL, logger)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	// Migrations run as the syncforge_app role so it owns the tables it will
	// read/write; the internal engine role receives grants from the migration.
	if err := db.Migrate(ctx, database.App, migrations.FS, logger); err != nil {
		logger.Error("migrations failed", "error", err)
		os.Exit(1)
	}

	metricsHandler, shutdownMetrics, err := observability.InitMetrics(ctx, "syncforge-engine")
	if err != nil {
		logger.Error("metrics init failed", "error", err)
		os.Exit(1)
	}
	defer shutdownMetrics()

	// Phase 1: the engine boots, owns migrations, and exposes health/metrics.
	// Phase 2+ adds the event processor, sync workers, retry and reconciliation.
	logger.Info("engine ready (workers arriving in Phase 2)")

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           engineHandler(metricsHandler),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("engine http listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		logger.Error("engine failed", "error", err)
		os.Exit(1)
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}
}

func engineHandler(metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"healthy","service":"engine"}`))
	})
	mux.Handle("GET /metrics", metricsHandler)
	return mux
}
