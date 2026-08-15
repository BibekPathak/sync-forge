package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"

	"syncforge/internal/store"
)

// newRawAPIKey generates a random credential. Only the hash is persisted; the
// raw key is returned to the caller exactly once.
func newRawAPIKey() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "sfk_" + hex.EncodeToString(buf), nil
}

type createKeyRequest struct {
	Name string `json:"name"`
	Role string `json:"role"`
}

// handleCreateAPIKey provisions a new API key for the caller's tenant. The raw
// key is shown once; only its hash is stored. ADMIN may mint any role up to
// its own rank.
func (s *Server) handleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	var req createKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	rank, ok := roleRank[req.Role]
	if !ok {
		writeError(w, http.StatusBadRequest, "role must be ADMIN, OPERATOR, DEVELOPER, or VIEWER")
		return
	}
	if rank > roleRank[roleFrom(r)] {
		writeError(w, http.StatusForbidden, "cannot mint a key above your own role")
		return
	}

	raw, err := newRawAPIKey()
	if err != nil {
		s.log.Error("generate api key", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to generate api key")
		return
	}
	k, err := store.CreateTenantAPIKey(r.Context(), s.db.App, tenantIDFrom(r), req.Name, req.Role, hashAPIKey(raw))
	if err != nil {
		s.log.Error("create api key", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create api key")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         k.ID,
		"tenant_id":  k.TenantID,
		"name":       k.Name,
		"role":       k.Role,
		"enabled":    k.Enabled,
		"created_at": k.CreatedAt,
		"api_key":    raw,
	})
}

func (s *Server) handleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := store.ListAPIKeys(r.Context(), s.db.App, tenantIDFrom(r))
	if err != nil {
		s.log.Error("list api keys", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list api keys")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

// handleRevokeAPIKey disables a key so it can no longer authenticate. The key
// used for the current request may not revoke itself.
func (s *Server) handleRevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	id := r.PathValue("id")
	if id == actorKeyID(r) {
		writeError(w, http.StatusBadRequest, "cannot revoke the key used for this request")
		return
	}
	if err := store.RevokeAPIKey(r.Context(), s.db.App, tenant, id); errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "api key not found")
		return
	} else if err != nil {
		s.log.Error("revoke api key", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to revoke api key")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "revoked", "id": id})
}
