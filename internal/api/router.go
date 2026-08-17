package api

import (
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Router assembles the API HTTP surface with middleware.
func (s *Server) Router(metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()

	// open endpoints
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.Handle("GET /api/v1/metrics", metricsHandler)

	// user authentication (login issues a signed session token)
	mux.HandleFunc("POST /api/v1/auth/login", s.handleLogin)
	mux.HandleFunc("POST /api/v1/auth/logout", s.handleLogout)
	mux.HandleFunc("POST /api/v1/auth/refresh", s.handleRefresh)

	// multi-factor auth (self-service; user session required)
	mux.Handle("POST /api/v1/auth/mfa/enroll", s.requireUser(http.HandlerFunc(s.handleMFAEnroll)))
	mux.Handle("POST /api/v1/auth/mfa/confirm", s.requireUser(http.HandlerFunc(s.handleMFAConfirm)))
	mux.Handle("POST /api/v1/auth/mfa/disable", s.requireUser(http.HandlerFunc(s.handleMFADisable)))

	// tenant management (platform provisioning: ADMIN role only)
	mux.Handle("POST /api/v1/tenants", s.requireRole("ADMIN")(http.HandlerFunc(s.handleCreateTenant)))
	mux.Handle("GET /api/v1/tenants", s.requireRole("ADMIN")(http.HandlerFunc(s.handleListTenants)))

	// api keys (ADMIN: create/list/revoke; the raw key is shown once)
	mux.Handle("POST /api/v1/keys", s.requireRole("ADMIN")(http.HandlerFunc(s.handleCreateAPIKey)))
	mux.Handle("GET /api/v1/keys", s.requireRole("ADMIN")(http.HandlerFunc(s.handleListAPIKeys)))
	mux.Handle("POST /api/v1/keys/{id}/revoke", s.requireRole("ADMIN")(http.HandlerFunc(s.handleRevokeAPIKey)))

	// users (ADMIN: create/list tenant login accounts)
	mux.Handle("POST /api/v1/users", s.requireRole("ADMIN")(http.HandlerFunc(s.handleCreateUser)))
	mux.Handle("GET /api/v1/users", s.requireRole("ADMIN")(http.HandlerFunc(s.handleListUsers)))

	// audit trail + applied-write ledger (read-only, VIEWER+)
	mux.Handle("GET /api/v1/audit", s.requireRole("VIEWER")(http.HandlerFunc(s.handleListAudit)))
	mux.Handle("GET /api/v1/operations", s.requireRole("VIEWER")(http.HandlerFunc(s.handleListOperations)))

	// tenant-scoped resources (API key auth + RLS + role gate)
	// VIEWER may read; DEVELOPER may configure connections and jobs.
	mux.Handle("GET /api/v1/connections", s.requireRole("VIEWER")(http.HandlerFunc(s.handleListConnections)))
	mux.Handle("POST /api/v1/connections", s.requireRole("DEVELOPER")(http.HandlerFunc(s.handleCreateConnection)))
	mux.Handle("GET /api/v1/connections/{id}", s.requireRole("VIEWER")(http.HandlerFunc(s.handleGetConnection)))

	// event inspection
	mux.Handle("GET /api/v1/sync-events", s.requireRole("VIEWER")(http.HandlerFunc(s.handleListSyncEvents)))
	mux.Handle("GET /api/v1/sync-events/{id}", s.requireRole("VIEWER")(http.HandlerFunc(s.handleGetSyncEvent)))

	// synchronization jobs (initial full sync, resumable)
	mux.Handle("POST /api/v1/sync-jobs", s.requireRole("DEVELOPER")(http.HandlerFunc(s.handleCreateSyncJob)))
	mux.Handle("GET /api/v1/sync-jobs", s.requireRole("VIEWER")(http.HandlerFunc(s.handleListSyncJobs)))
	mux.Handle("GET /api/v1/sync-jobs/{id}", s.requireRole("VIEWER")(http.HandlerFunc(s.handleGetSyncJob)))
	mux.Handle("POST /api/v1/sync-jobs/{id}/run", s.requireRole("OPERATOR")(http.HandlerFunc(s.handleRerunSyncJob)))

	// dead-letter queue (inspect, retry, discard)
	mux.Handle("GET /api/v1/dlq", s.requireRole("VIEWER")(http.HandlerFunc(s.handleListDLQ)))
	mux.Handle("GET /api/v1/dlq/{id}", s.requireRole("VIEWER")(http.HandlerFunc(s.handleGetDLQ)))
	mux.Handle("POST /api/v1/dlq/{id}/retry", s.requireRole("OPERATOR")(http.HandlerFunc(s.handleDLQRetry)))
	mux.Handle("POST /api/v1/dlq/{id}/discard", s.requireRole("OPERATOR")(http.HandlerFunc(s.handleDLQDiscard)))

	// conflicts (inspect, resolve, dismiss)
	mux.Handle("GET /api/v1/conflicts", s.requireRole("VIEWER")(http.HandlerFunc(s.handleListConflicts)))
	mux.Handle("GET /api/v1/conflicts/{id}", s.requireRole("VIEWER")(http.HandlerFunc(s.handleGetConflict)))
	mux.Handle("POST /api/v1/conflicts/{id}/resolve", s.requireRole("OPERATOR")(http.HandlerFunc(s.handleResolveConflict)))
	mux.Handle("POST /api/v1/conflicts/{id}/dismiss", s.requireRole("OPERATOR")(http.HandlerFunc(s.handleDismissConflict)))

	// reconciliation (run sweeps, review findings, apply/dismiss repairs)
	mux.Handle("POST /api/v1/reconciliations", s.requireRole("OPERATOR")(http.HandlerFunc(s.handleCreateReconciliation)))
	mux.Handle("GET /api/v1/reconciliations", s.requireRole("VIEWER")(http.HandlerFunc(s.handleListReconciliations)))
	mux.Handle("GET /api/v1/reconciliations/{id}", s.requireRole("VIEWER")(http.HandlerFunc(s.handleGetReconciliation)))
	mux.Handle("GET /api/v1/reconciliations/{id}/findings", s.requireRole("VIEWER")(http.HandlerFunc(s.handleListReconcileFindings)))
	mux.Handle("POST /api/v1/reconciliations/{id}/findings/{findingId}/apply", s.requireRole("OPERATOR")(http.HandlerFunc(s.handleApplyFinding)))
	mux.Handle("POST /api/v1/reconciliations/{id}/findings/{findingId}/dismiss", s.requireRole("OPERATOR")(http.HandlerFunc(s.handleDismissFinding)))

	// webhook gateway (HMAC-signed)
	mux.HandleFunc("POST /webhooks/{provider}/{tenant_slug}", s.handleWebhook)

	return s.middleware(mux)
}

// middleware applies recover, request logging, metrics, tracing and CORS.
func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		ctx, span := otel.Tracer("api").Start(r.Context(), "http.request",
			trace.WithAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.String("url.path", r.URL.Path),
			))
		defer span.End()
		r = r.WithContext(ctx)

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
			span.SetAttributes(attribute.Int("http.response.status_code", sw.status))
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
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-API-Key")
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
