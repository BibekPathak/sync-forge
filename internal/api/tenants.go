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
	writeJSON(w, http.StatusCreated, t)
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
