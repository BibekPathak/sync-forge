package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"syncforge/internal/store"
)

type createTenantRequest struct {
	Name string `json:"name"`
	Slug string `json:"slug"`
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	var req createTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" || req.Slug == "" {
		writeError(w, http.StatusBadRequest, "name and slug are required")
		return
	}
	t, err := store.CreateTenant(r.Context(), s.db.Admin, req.Name, req.Slug)
	if errors.Is(err, store.ErrExists) {
		writeError(w, http.StatusConflict, "tenant slug already exists")
		return
	}
	if err != nil {
		s.log.Error("create tenant", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create tenant")
		return
	}

	// Provision the tenant's initial ADMIN key and return the raw credential
	// exactly once; only its hash is stored.
	raw, err := newRawAPIKey()
	if err != nil {
		s.log.Error("generate tenant admin key", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to provision admin key")
		return
	}
	if _, err := store.CreateAPIKey(r.Context(), s.db.Admin, t.ID, "initial-admin", "ADMIN", hashAPIKey(raw)); err != nil {
		s.log.Error("create tenant admin key", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to provision admin key")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         t.ID,
		"name":       t.Name,
		"slug":       t.Slug,
		"status":     t.Status,
		"created_at": t.CreatedAt,
		"api_key":    raw,
	})
}

func (s *Server) handleListTenants(w http.ResponseWriter, r *http.Request) {
	tenants, err := store.ListTenants(r.Context(), s.db.Admin)
	if err != nil {
		s.log.Error("list tenants", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list tenants")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
}
