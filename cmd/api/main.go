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

	"syncforge/internal/api"
	"syncforge/internal/cache"
	"syncforge/internal/config"
	"syncforge/internal/db"
	"syncforge/internal/observability"
	"syncforge/migrations"

	"go.opentelemetry.io/otel"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := config.Load()
	cfg.Service = "api"
	logger.Info("starting api", "config", cfg.String())

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

	c := cache.New(cfg.RedisAddr)
	if err := c.Ping(ctx); err != nil {
		logger.Warn("redis unavailable (continuing)", "error", err)
	} else {
		logger.Info("redis connected")
	}
	defer c.Close()

	metricsHandler, shutdownMetrics, err := observability.InitMetrics(ctx, "syncforge-api")
	if err != nil {
		logger.Error("metrics init failed", "error", err)
		os.Exit(1)
	}
	defer shutdownMetrics()

	metrics, err := observability.NewServiceMetrics(otel.Meter("syncforge-api"))
	if err != nil {
		logger.Error("metrics init failed", "error", err)
		os.Exit(1)
	}

	shutdownTracing, err := observability.InitTracing(ctx, "syncforge-api", cfg.OTLPEndpoint)
	if err != nil {
		logger.Error("tracing init failed", "error", err)
		os.Exit(1)
	}
	defer shutdownTracing()

	srv := api.New(cfg, database, c, metrics, logger)

	if cfg.SeedAcme {
		if err := srv.SeedDemoTenant(ctx); err != nil {
			logger.Error("seed failed", "error", err)
			os.Exit(1)
		}
		logger.Info("demo tenant seeded", "api_key", "sfk_acme_dev")
	}

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Router(metricsHandler),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("api listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		logger.Error("api server failed", "error", err)
		os.Exit(1)
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}
}
