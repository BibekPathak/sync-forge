package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"syncforge/internal/store"
)

type createSyncJobRequest struct {
	Entity      string `json:"entity"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

// handleCreateSyncJob registers a pending full-sync job. The engine's job
// runner picks it up (and checkpoints progress so crashing resumes).
func (s *Server) handleCreateSyncJob(w http.ResponseWriter, r *http.Request) {
	var req createSyncJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Entity == "" {
		req.Entity = "customer"
	}
	if req.Source == "" || req.Destination == "" {
		writeError(w, http.StatusBadRequest, "source and destination are required")
		return
	}
	job, err := store.CreateSyncJob(r.Context(), s.db.App, store.SyncJob{
		TenantID:    tenantIDFrom(r),
		Entity:      req.Entity,
		Source:      req.Source,
		Destination: req.Destination,
		BatchSize:   s.cfg.SyncJobBatchSize,
	})
	if err != nil {
		s.log.Error("create sync job", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create sync job")
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleListSyncJobs(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	jobs, err := store.ListSyncJobs(r.Context(), s.db.App, tenantIDFrom(r), limit)
	if err != nil {
		s.log.Error("list sync jobs", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list sync jobs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) handleGetSyncJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := store.GetSyncJob(r.Context(), s.db.App, tenantIDFrom(r), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "sync job not found")
		return
	}
	if err != nil {
		s.log.Error("get sync job", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get sync job")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// handleRerunSyncJob reopens a failed (or paused) job so the runner resumes it
// from its last checkpoint instead of re-fetching from zero.
func (s *Server) handleRerunSyncJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := store.GetSyncJob(r.Context(), s.db.App, tenantIDFrom(r), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "sync job not found")
		return
	}
	if err != nil {
		s.log.Error("get sync job for rerun", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to reopen sync job")
		return
	}
	if job.Status == "running" {
		writeError(w, http.StatusConflict, "sync job is already running")
		return
	}
	// Reopen to pending, keep cursor/counters for a resumable run. The helper
	// uses the admin pool to bypass RLS when clearing the running flag.
	if _, err := s.db.Admin.Exec(r.Context(),
		`UPDATE sync_jobs SET status='pending', finished_at=NULL, last_error=NULL WHERE id=$1 AND tenant_id=$2`,
		job.ID, job.TenantID); err != nil {
		s.log.Error("reopen sync job", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to reopen sync job")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "pending", "id": job.ID})
}
