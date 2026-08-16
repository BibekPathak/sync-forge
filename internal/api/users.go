package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"golang.org/x/crypto/bcrypt"

	"syncforge/internal/store"
)

type createUserRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Role     string `json:"role"`
}

// handleCreateUser provisions a user account for the caller's tenant (ADMIN
// only). The password is bcrypt-hashed before storage.
func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	var req createUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "email and password are required")
		return
	}
	rank, ok := roleRank[req.Role]
	if !ok {
		writeError(w, http.StatusBadRequest, "role must be ADMIN, OPERATOR, DEVELOPER, or VIEWER")
		return
	}
	if rank > roleRank[roleFrom(r)] {
		writeError(w, http.StatusForbidden, "cannot create a user above your own role")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		s.log.Error("hash password", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create user")
		return
	}
	u, err := store.CreateUser(r.Context(), s.db.App, tenantIDFrom(r), req.Email, string(hash), req.Role)
	if errors.Is(err, store.ErrExists) {
		writeError(w, http.StatusConflict, "user already exists")
		return
	}
	if err != nil {
		s.log.Error("create user", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create user")
		return
	}
	writeJSON(w, http.StatusCreated, u)
}

func (s *Server) handleListUsers(w http.ResponseWriter, r *http.Request) {
	users, err := store.ListUsers(r.Context(), s.db.App, tenantIDFrom(r))
	if err != nil {
		s.log.Error("list users", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list users")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": users})
}
