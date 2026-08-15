package api

import (
	"errors"
	"net/http"
	"strconv"

	"syncforge/internal/store"
)

// handleListSyncEvents returns recently ingested events for the tenant, newest
// first.
func (s *Server) handleListSyncEvents(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	events, err := store.ListSourceEvents(r.Context(), s.db.App, tenantIDFrom(r), limit)
	if err != nil {
		s.log.Error("list sync events", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list sync events")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

// handleGetSyncEvent returns one ingested event by event id.
func (s *Server) handleGetSyncEvent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ev, err := store.GetSourceEvent(r.Context(), s.db.App, tenantIDFrom(r), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "event not found")
		return
	}
	if err != nil {
		s.log.Error("get sync event", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get sync event")
		return
	}
	writeJSON(w, http.StatusOK, ev)
}
