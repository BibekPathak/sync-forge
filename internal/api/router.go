package api

import (
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Router assembles the API HTTP surface with middleware.
func (s *Server) Router(metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()

	// open endpoints
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.Handle("GET /api/v1/metrics", metricsHandler)

	// tenant management (bootstrap key until Phase 7 RBAC)
	mux.Handle("POST /api/v1/tenants", s.requireBootstrap(http.HandlerFunc(s.handleCreateTenant)))
	mux.Handle("GET /api/v1/tenants", s.requireBootstrap(http.HandlerFunc(s.handleListTenants)))

	// tenant-scoped resources (API key auth + RLS)
	mux.Handle("GET /api/v1/connections", s.requireAPIKey(http.HandlerFunc(s.handleListConnections)))
	mux.Handle("POST /api/v1/connections", s.requireAPIKey(http.HandlerFunc(s.handleCreateConnection)))
	mux.Handle("GET /api/v1/connections/{id}", s.requireAPIKey(http.HandlerFunc(s.handleGetConnection)))

	// event inspection
	mux.Handle("GET /api/v1/sync-events", s.requireAPIKey(http.HandlerFunc(s.handleListSyncEvents)))
	mux.Handle("GET /api/v1/sync-events/{id}", s.requireAPIKey(http.HandlerFunc(s.handleGetSyncEvent)))

	// synchronization jobs (initial full sync, resumable)
	mux.Handle("POST /api/v1/sync-jobs", s.requireAPIKey(http.HandlerFunc(s.handleCreateSyncJob)))
	mux.Handle("GET /api/v1/sync-jobs", s.requireAPIKey(http.HandlerFunc(s.handleListSyncJobs)))
	mux.Handle("GET /api/v1/sync-jobs/{id}", s.requireAPIKey(http.HandlerFunc(s.handleGetSyncJob)))
	mux.Handle("POST /api/v1/sync-jobs/{id}/run", s.requireAPIKey(http.HandlerFunc(s.handleRerunSyncJob)))

	// dead-letter queue (inspect, retry, discard)
	mux.Handle("GET /api/v1/dlq", s.requireAPIKey(http.HandlerFunc(s.handleListDLQ)))
	mux.Handle("GET /api/v1/dlq/{id}", s.requireAPIKey(http.HandlerFunc(s.handleGetDLQ)))
	mux.Handle("POST /api/v1/dlq/{id}/retry", s.requireAPIKey(http.HandlerFunc(s.handleDLQRetry)))
	mux.Handle("POST /api/v1/dlq/{id}/discard", s.requireAPIKey(http.HandlerFunc(s.handleDLQDiscard)))

	// conflicts (inspect, resolve, dismiss)
	mux.Handle("GET /api/v1/conflicts", s.requireAPIKey(http.HandlerFunc(s.handleListConflicts)))
	mux.Handle("GET /api/v1/conflicts/{id}", s.requireAPIKey(http.HandlerFunc(s.handleGetConflict)))
	mux.Handle("POST /api/v1/conflicts/{id}/resolve", s.requireAPIKey(http.HandlerFunc(s.handleResolveConflict)))
	mux.Handle("POST /api/v1/conflicts/{id}/dismiss", s.requireAPIKey(http.HandlerFunc(s.handleDismissConflict)))

	// webhook gateway (HMAC-signed)
	mux.HandleFunc("POST /webhooks/{provider}/{tenant_slug}", s.handleWebhook)

	return s.middleware(mux)
}

// middleware applies recover, request logging, metrics and CORS.
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		s.metrics.HTTPInflight.Add(r.Context(), 1)
		defer s.metrics.HTTPInflight.Add(r.Context(), -1)

		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic", "err", rec, "stack", string(debug.Stack()))
				writeError(sw, http.StatusInternalServerError, "internal error")
			}
			dur := time.Since(start).Seconds()
			attrs := []attribute.KeyValue{
				attribute.String("method", r.Method),
				attribute.String("path", r.URL.Path),
				attribute.String("status", strconv.Itoa(sw.status)),
			}
			s.metrics.HTTPRequests.Add(r.Context(), 1, metric.WithAttributes(attrs...))
			s.metrics.HTTPDuration.Record(r.Context(), dur, metric.WithAttributes(attrs...))
			s.log.Info("http",
				"method", r.Method,
				"path", r.URL.Path,
				"status", sw.status,
				"duration_ms", dur*1000,
			)
		}()

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-API-Key, X-Bootstrap-Key")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(sw, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
