package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"syncforge/internal/events"
	"syncforge/internal/store"
)

type createReconciliationRequest struct {
	Entity string `json:"entity"`
	Source string `json:"source"`
	Mode   string `json:"mode"`
}

// handleCreateReconciliation starts a reconciliation sweep: it registers a
// reconcile-type sync job and a run row. The engine's sync job runner picks the
// job up; auto runs repair divergences inline, manual runs park findings for
// operator approval.
func (s *Server) handleCreateReconciliation(w http.ResponseWriter, r *http.Request) {
	var req createReconciliationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Entity == "" {
		req.Entity = "customer"
	}
	if req.Source == "" {
		writeError(w, http.StatusBadRequest, "source is required")
		return
	}
	if req.Mode != "auto" && req.Mode != "manual" {
		req.Mode = "auto"
	}

	job, err := store.CreateSyncJob(r.Context(), s.db.App, store.SyncJob{
		TenantID:  tenantIDFrom(r),
		Entity:    req.Entity,
		Source:    req.Source,
		Type:      "reconcile",
		BatchSize: s.cfg.SyncJobBatchSize,
	})
	if err != nil {
		s.log.Error("create reconcile job", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create reconciliation")
		return
	}
	run, err := store.CreateReconciliationRun(r.Context(), s.db.App, store.ReconciliationRun{
		TenantID: tenantIDFrom(r),
		Entity:   req.Entity,
		Source:   req.Source,
		Mode:     req.Mode,
		Status:   store.ReconcileRunning,
		JobID:    &job.ID,
	})
	if err != nil {
		s.log.Error("create reconcile run", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create reconciliation run")
		return
	}
	writeJSON(w, http.StatusCreated, renderRun(run))
}

func (s *Server) handleListReconciliations(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	runs, err := store.ListReconciliationRuns(r.Context(), s.db.App, tenantIDFrom(r), limit)
	if err != nil {
		s.log.Error("list reconciliations", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list reconciliations")
		return
	}
	out := make([]map[string]any, 0, len(runs))
	for _, run := range runs {
		out = append(out, renderRun(run))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "count": len(out)})
}

func (s *Server) handleGetReconciliation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := store.GetReconciliationRun(r.Context(), s.db.App, tenantIDFrom(r), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "reconciliation not found")
		return
	}
	if err != nil {
		s.log.Error("get reconciliation", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get reconciliation")
		return
	}
	writeJSON(w, http.StatusOK, renderRun(run))
}

func (s *Server) handleListReconcileFindings(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	status := r.URL.Query().Get("status")
	items, err := store.ListReconciliationFindings(r.Context(), s.db.App, tenantIDFrom(r), runID, status, limit)
	if err != nil {
		s.log.Error("list reconcile findings", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list findings")
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, f := range items {
		out = append(out, renderFinding(f))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "count": len(out)})
}

type applyFindingRequest struct {
	Direction string `json:"direction"`
}

// handleApplyFinding applies a pending reconciliation finding. The operator may
// override the repair direction. The chosen direction is persisted and applied
// durably through the retry queue: the worker re-applies the same deterministic
// event id, so a crash mid-apply resumes without double-applying.
func (s *Server) handleApplyFinding(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	findingID := r.PathValue("findingId")

	f, err := store.GetReconciliationFinding(r.Context(), s.db.App, tenant, findingID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "finding not found")
		return
	}
	if err != nil {
		s.log.Error("get finding for apply", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load finding")
		return
	}
	if f.Status != store.FindingPending {
		writeError(w, http.StatusConflict, "finding is not pending")
		return
	}

	run, err := store.GetReconciliationRun(r.Context(), s.db.App, tenant, f.RunID)
	if err != nil {
		s.log.Error("get run for finding apply", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load run")
		return
	}

	direction := f.Direction
	var body applyFindingRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err == nil && body.Direction != "" {
		switch body.Direction {
		case "push_canonical", "adopt_provider", "delete":
			direction = body.Direction
			if err := store.SetReconciliationFindingDirection(r.Context(), s.db.App, tenant, f.ID, direction); err != nil {
				s.log.Error("override finding direction", "error", err)
				writeError(w, http.StatusInternalServerError, "failed to apply direction")
				return
			}
		default:
			writeError(w, http.StatusBadRequest, "direction must be push_canonical, adopt_provider, or delete")
			return
		}
	}

	ev := events.Event{
		EventID:       reconcileEventID(f.ID),
		TenantID:      tenant,
		Source:        run.Source,
		EntityType:    run.Entity,
		EntityID:      f.ProviderID,
		EventType:     events.EventUpdated,
		SourceVersion: f.ProviderVersion,
		OccurredAt:    time.Now().UTC(),
		ReceivedAt:    time.Now().UTC(),
		Provenance:    events.Provenance{ReconcileFindingID: f.ID},
		Payload:       map[string]any{},
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		s.log.Error("marshal reconcile apply event", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to encode repair")
		return
	}
	if _, err := store.EnqueueRetry(r.Context(), s.db.App, store.RetryEntry{
		TenantID:    tenant,
		EventID:     ev.EventID,
		MaxAttempts: s.cfg.RetryMaxAttempts,
		LastError:   "operator reconcile apply",
		ErrorClass:  "RECONCILIATION",
		State:       payload,
	}, 0); err != nil {
		s.log.Error("enqueue reconcile apply retry", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to enqueue repair")
		return
	}
	s.audit(r, "finding.apply", "reconciliation_finding", f.ID, map[string]any{
		"run_id": f.RunID, "kind": f.Kind, "direction": direction, "provider_id": f.ProviderID,
	})
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":    "apply_queued",
		"id":        f.ID,
		"direction": direction,
		"queued_at": time.Now().UTC(),
	})
}

// handleDismissFinding dismisses a pending finding without applying any repair.
// The operator's decision is immutable.
func (s *Server) handleDismissFinding(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	findingID := r.PathValue("findingId")

	f, err := store.GetReconciliationFinding(r.Context(), s.db.App, tenant, findingID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "finding not found")
		return
	}
	if err != nil {
		s.log.Error("get finding for dismiss", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load finding")
		return
	}
	if f.Status != store.FindingPending {
		writeError(w, http.StatusConflict, "finding is not pending")
		return
	}
	if err := store.SetReconciliationFindingStatus(r.Context(), s.db.App, tenant, findingID, store.FindingDismissed, nil); err != nil {
		s.log.Error("dismiss finding", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to dismiss finding")
		return
	}
	s.audit(r, "finding.dismiss", "reconciliation_finding", findingID, map[string]any{
		"run_id": f.RunID, "kind": f.Kind, "provider_id": f.ProviderID,
	})
	writeJSON(w, http.StatusOK, map[string]any{"status": "dismissed", "id": findingID})
}

func renderRun(run store.ReconciliationRun) map[string]any {
	return map[string]any{
		"id":          run.ID,
		"tenant_id":   run.TenantID,
		"entity":      run.Entity,
		"source":      run.Source,
		"mode":        run.Mode,
		"status":      run.Status,
		"job_id":      run.JobID,
		"total":       run.Total,
		"drift":       run.Drift,
		"missed":      run.Missed,
		"deleted":     run.Deleted,
		"started_at":  run.StartedAt,
		"finished_at": run.FinishedAt,
	}
}

func renderFinding(f store.ReconciliationFinding) map[string]any {
	return map[string]any{
		"id":               f.ID,
		"run_id":           f.RunID,
		"tenant_id":        f.TenantID,
		"kind":             f.Kind,
		"provider_id":      f.ProviderID,
		"canonical_id":     f.CanonicalID,
		"canonical_fields": f.CanonicalFields,
		"provider_fields":  f.ProviderFields,
		"provider_version": f.ProviderVersion,
		"direction":        f.Direction,
		"status":           f.Status,
		"error":            f.Error,
		"created_at":       f.CreatedAt,
		"applied_at":       f.AppliedAt,
	}
}

// reconcileEventID is the deterministic idempotency key for a reconcile repair.
// The engine and the operator apply path share it so the worker's claim makes
// both exactly-once.
func reconcileEventID(findingID string) string {
	return "reconcile:" + findingID
}
