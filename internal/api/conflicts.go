package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"syncforge/internal/events"
	"syncforge/internal/store"
)

// handleListConflicts returns conflicts for the tenant, optionally filtered by
// status (CONFLICT_PENDING / RESOLVED / AUTO_RESOLVED / DISMISSED).
func (s *Server) handleListConflicts(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	status := r.URL.Query().Get("status")
	items, err := store.ListConflicts(r.Context(), s.db.App, tenantIDFrom(r), status, limit)
	if err != nil {
		s.log.Error("list conflicts", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list conflicts")
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, c := range items {
		out = append(out, renderConflict(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "count": len(out)})
}

// handleGetConflict returns a single conflict with both sides' payloads
// decoded as JSON so the operator can inspect the exact field snapshots.
func (s *Server) handleGetConflict(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	c, err := store.GetConflict(r.Context(), s.db.App, tenantIDFrom(r), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "conflict not found")
		return
	}
	if err != nil {
		s.log.Error("get conflict", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get conflict")
		return
	}
	writeJSON(w, http.StatusOK, renderConflict(c))
}

// handleResolveConflict picks a winning side for a CONFLICT_PENDING conflict
// and enqueues its application as a durable resolution event. The worker
// applies the chosen side's canonical fields to every destination; success
// transitions the conflict to RESOLVED.
func (s *Server) handleResolveConflict(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	id := r.PathValue("id")

	c, err := store.GetConflict(r.Context(), s.db.App, tenant, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "conflict not found")
		return
	}
	if err != nil {
		s.log.Error("get conflict for resolve", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load conflict")
		return
	}
	if c.Status != store.ConflictPending {
		writeError(w, http.StatusConflict, "conflict is not pending resolution")
		return
	}

	var body struct {
		Side string `json:"side"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if body.Side != "a" && body.Side != "b" {
		writeError(w, http.StatusBadRequest, "side must be \"a\" or \"b\"")
		return
	}

	// Chosen side becomes the winning source carried by the resolution event.
	var (
		src     string
		version int64
		state   []byte
	)
	if body.Side == "a" {
		src, version, state = c.SourceA, c.VersionA, c.PayloadA
	} else {
		src, version, state = c.SourceB, c.VersionB, c.PayloadB
	}

	ev := events.Event{
		EventID:       "resolve:" + c.ID,
		TenantID:      tenant,
		Source:        src,
		EntityType:    c.EntityType,
		EntityID:      c.EntityID, // canonical id: applyResolution reads it directly
		EventType:     events.EventUpdated,
		SourceVersion: version,
		OccurredAt:    time.Now().UTC(),
		ReceivedAt:    time.Now().UTC(),
		Provenance:    events.Provenance{ResolvedConflictID: c.ID},
		Payload: map[string]any{
			"fields": decodedPayload(state),
		},
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		s.log.Error("marshal resolution event", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to encode resolution")
		return
	}

	if _, err := store.EnqueueRetry(r.Context(), s.db.App, store.RetryEntry{
		TenantID:    tenant,
		EventID:     ev.EventID,
		MaxAttempts: s.cfg.RetryMaxAttempts,
		LastError:   "operator resolution",
		ErrorClass:  "RESOLUTION",
		State:       payload,
	}, 0); err != nil {
		s.log.Error("enqueue resolution retry", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to enqueue resolution")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":    "resolution_queued",
		"id":        c.ID,
		"winner":    src,
		"queued_at": time.Now().UTC(),
	})
}

// handleDismissConflict dismisses a pending conflict without applying either
// side. The operator's decision is immutable: a dismissed conflict is never
// re-applied.
func (s *Server) handleDismissConflict(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	id := r.PathValue("id")

	c, err := store.GetConflict(r.Context(), s.db.App, tenant, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "conflict not found")
		return
	}
	if err != nil {
		s.log.Error("get conflict for dismiss", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load conflict")
		return
	}
	if c.Status != store.ConflictPending {
		writeError(w, http.StatusConflict, "conflict is not pending")
		return
	}

	if err := store.SetConflictStatus(r.Context(), s.db.App, tenant, id, store.ConflictDismissed, c.ResolutionStrategy, "operator"); err != nil {
		s.log.Error("dismiss conflict", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to dismiss conflict")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"status": "dismissed", "id": id})
}

// renderConflict decodes both payloads into JSON maps for operator display.
func renderConflict(c store.ConflictRecord) map[string]any {
	return map[string]any{
		"id":                  c.ID,
		"tenant_id":           c.TenantID,
		"entity_type":         c.EntityType,
		"entity_id":           c.EntityID,
		"source_a":            c.SourceA,
		"version_a":           c.VersionA,
		"payload_a":           decodedPayload(c.PayloadA),
		"source_b":            c.SourceB,
		"version_b":           c.VersionB,
		"payload_b":           decodedPayload(c.PayloadB),
		"detected_at":         c.DetectedAt,
		"status":              c.Status,
		"resolution_strategy": c.ResolutionStrategy,
		"resolved_by":         c.ResolvedBy,
		"resolved_at":         c.ResolvedAt,
	}
}

// decodedPayload turns a stored JSON snapshot into a map for responses.
func decodedPayload(b []byte) map[string]any {
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{"raw": string(bytes.TrimSpace(b))}
	}
	return m
}
