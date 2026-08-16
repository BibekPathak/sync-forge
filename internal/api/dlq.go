package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"syncforge/internal/store"
)

// handleListDLQ returns dead-letter events for the tenant, optionally filtered
// by status.
func (s *Server) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	status := r.URL.Query().Get("status")
	items, err := store.ListDeadLetters(r.Context(), s.db.App, tenantIDFrom(r), status, limit)
	if err != nil {
		s.log.Error("list dlq", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list dead letters")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

// handleGetDLQ returns a single dead-letter entry with its original payload.
func (s *Server) handleGetDLQ(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	item, err := store.GetDeadLetter(r.Context(), s.db.App, tenantIDFrom(r), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "dead letter not found")
		return
	}
	if err != nil {
		s.log.Error("get dlq", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get dead letter")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

// handleDLQRetry re-enqueues a dead-letter event as a fresh retry: the stored
// canonical event is scheduled with no delay, so the retry engine immediately
// re-processes it. Success resolves the entry; failure re-parks it.
func (s *Server) handleDLQRetry(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	id := r.PathValue("id")
	item, err := store.GetDeadLetter(r.Context(), s.db.App, tenant, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "dead letter not found")
		return
	}
	if err != nil {
		s.log.Error("get dlq for retry", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load dead letter")
		return
	}
	if err := store.SetDeadLetterStatus(r.Context(), s.db.App, tenant, id, "retrying"); err != nil {
		s.log.Error("mark dlq retrying", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to update dead letter")
		return
	}
	if _, err := store.EnqueueRetry(r.Context(), s.db.App, store.RetryEntry{
		TenantID:    item.TenantID,
		EventID:     item.EventID,
		MaxAttempts: s.cfg.RetryMaxAttempts,
		LastError:   "manual replay from DLQ",
		ErrorClass:  item.ErrorClass,
		State:       item.Payload,
	}, 0); err != nil {
		s.log.Error("enqueue dlq retry", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to enqueue retry")
		return
	}
	s.audit(r, "dlq.retry", "dead_letter", item.ID, map[string]any{"event_id": item.EventID, "error_class": item.ErrorClass})
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "retrying",
		"id":        item.ID,
		"event_id":  item.EventID,
		"queued_at": time.Now().UTC(),
	})
}

// handleDLQDiscard marks a dead-letter event as discarded (operator decided it
// should never be applied).
func (s *Server) handleDLQDiscard(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	id := r.PathValue("id")
	item, err := store.GetDeadLetter(r.Context(), s.db.App, tenant, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "dead letter not found")
		return
	}
	if err != nil {
		s.log.Error("get dlq for discard", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load dead letter")
		return
	}
	if err := store.SetDeadLetterStatus(r.Context(), s.db.App, tenant, id, "discarded"); err != nil {
		s.log.Error("discard dlq", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to update dead letter")
		return
	}
	s.audit(r, "dlq.discard", "dead_letter", item.ID, map[string]any{"event_id": item.EventID, "error_class": item.ErrorClass})
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "discarded",
		"id":       item.ID,
		"event_id": item.EventID,
	})
}
