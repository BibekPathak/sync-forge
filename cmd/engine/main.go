package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"

	"syncforge/internal/config"
	"syncforge/internal/db"
	"syncforge/internal/eventbus"
	"syncforge/internal/ingestion"
	"syncforge/internal/observability"
	"syncforge/internal/syncworker"
	"syncforge/migrations"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := config.Load()
	cfg.Service = "engine"
	logger.Info("starting engine", "config", cfg.String(), "kafka", cfg.KafkaBrokers)

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

	syncMetrics, err := observability.NewSyncMetrics(otel.Meter("syncforge-engine"))
	if err != nil {
		logger.Error("sync metrics init failed", "error", err)
		os.Exit(1)
	}

	var bus eventbus.Bus
	if strings.TrimSpace(cfg.KafkaBrokers) == "" || cfg.KafkaBrokers == "memory" {
		bus = eventbus.NewMemoryBus(logger)
		logger.Info("using in-memory event bus (no broker)")
	} else {
		brokers := strings.Split(cfg.KafkaBrokers, ",")
		bus, err = eventbus.NewRedpanda(brokers, logger)
		if err != nil {
			logger.Error("event bus init failed", "error", err)
			os.Exit(1)
		}
		logger.Info("using redpanda event bus", "brokers", cfg.KafkaBrokers)
	}
	defer bus.Close()

	worker := syncworker.New(database, syncMetrics, logger)
	processor := ingestion.New(database, bus, logger)

	errCh := make(chan error, 2)
	go func() {
		errCh <- processor.Run(ctx)
	}()
	go func() {
		errCh <- bus.Subscribe(ctx, eventbus.TopicSyncEvents, cfg.KafkaGroupID, worker.Handle)
	}()

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           engineHandler(metricsHandler, database, bus),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		logger.Info("engine http listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		logger.Error("engine failed", "error", err)
		stop()
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}

func engineHandler(metricsHandler http.Handler, database *db.DB, bus eventbus.Bus) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		hctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		dbStatus := "ok"
		if err := database.App.Ping(hctx); err != nil {
			dbStatus = "error"
		}
		busStatus := "ok"
		if err := bus.Health(hctx); err != nil {
			busStatus = "error"
		}
		status := http.StatusOK
		if dbStatus != "ok" || busStatus != "ok" {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"` + map[bool]string{true: "healthy", false: "degraded"}[status == http.StatusOK] + `","database":"` + dbStatus + `","bus":"` + busStatus + `","service":"engine"}`))
	})
	mux.Handle("GET /metrics", metricsHandler)
	return mux
}
