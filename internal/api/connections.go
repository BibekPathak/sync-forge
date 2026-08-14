package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"syncforge/internal/store"
)

type createConnectionRequest struct {
	Name          string         `json:"name"`
	Provider      string         `json:"provider"`
	BaseURL       string         `json:"base_url"`
	WebhookSecret string         `json:"webhook_secret,omitempty"`
	Config        map[string]any `json:"config,omitempty"`
}

func (s *Server) handleCreateConnection(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantIDFrom(r)
	var req createConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" || req.Provider == "" || req.BaseURL == "" {
		writeError(w, http.StatusBadRequest, "name, provider and base_url are required")
		return
	}
	if req.Config == nil {
		req.Config = map[string]any{}
	}
	c, err := store.CreateConnection(r.Context(), s.db.App, store.Connection{
		TenantID:      tenantID,
		Name:          req.Name,
		Provider:      req.Provider,
		BaseURL:       req.BaseURL,
		Status:        "disconnected",
		WebhookSecret: req.WebhookSecret,
		Config:        req.Config,
	})
	if err != nil {
		s.log.Error("create connection", "tenant", tenantID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create connection")
		return
	}
	writeJSON(w, http.StatusCreated, c)
}

func (s *Server) handleListConnections(w http.ResponseWriter, r *http.Request) {
	conns, err := store.ListConnections(r.Context(), s.db.App, tenantIDFrom(r))
	if err != nil {
		s.log.Error("list connections", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list connections")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"connections": conns})
}

func (s *Server) handleGetConnection(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	c, err := store.GetConnection(r.Context(), s.db.App, tenantIDFrom(r), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "connection not found")
		return
	}
	if err != nil {
		s.log.Error("get connection", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get connection")
		return
	}
	writeJSON(w, http.StatusOK, c)
}
