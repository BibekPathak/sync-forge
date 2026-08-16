package api

import (
	"net/http"
	"strconv"

	"syncforge/internal/store"
)

// handleListAudit returns the tenant's audit trail, newest first, with optional
// actor/action/resource filters. Read-only; operator actions are written by
// the handlers that perform them.
func (s *Server) handleListAudit(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	items, err := store.ListAuditLogs(r.Context(), s.db.App, tenantIDFrom(r),
		r.URL.Query().Get("actor"), r.URL.Query().Get("action"), r.URL.Query().Get("resource"), limit)
	if err != nil {
		s.log.Error("list audit log", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list audit log")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

// handleListOperations returns the ledger of applied destination writes, newest
// first, optionally filtered by entity or target source.
func (s *Server) handleListOperations(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	items, err := store.ListSyncOperations(r.Context(), s.db.App, tenantIDFrom(r),
		r.URL.Query().Get("entity_type"), r.URL.Query().Get("entity_id"), r.URL.Query().Get("target_source"), limit)
	if err != nil {
		s.log.Error("list sync operations", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list sync operations")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}
