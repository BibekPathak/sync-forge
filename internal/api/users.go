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
	s.audit(r, "user.create", "user", u.ID, map[string]any{"email": u.Email, "role": u.Role})
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

type changePasswordRequest struct {
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
}

// handleChangePassword lets a user change their own password. The current
// password must be verified, and a change revokes every other session so a
// leaked token cannot outlive the password change.
func (s *Server) handleChangePassword(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	userID := userIDFrom(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "user session required")
		return
	}
	var req changePasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CurrentPassword == "" || req.NewPassword == "" {
		writeError(w, http.StatusBadRequest, "current_password and new_password are required")
		return
	}
	user, err := store.GetUser(r.Context(), s.db.App, tenant, userID)
	if err != nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.CurrentPassword)); err != nil {
		writeError(w, http.StatusUnauthorized, "current password is incorrect")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		s.log.Error("hash password", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to change password")
		return
	}
	if err := store.SetUserPassword(r.Context(), s.db.App, tenant, userID, string(hash)); err != nil {
		s.log.Error("set user password", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to change password")
		return
	}
	// Revoke the current session too, forcing a fresh login with the new
	// password.
	claims, err := s.verifySessionToken(r, bearerTokenFrom(r))
	if err == nil {
		_ = store.RevokeSession(r.Context(), s.db.App, tenant, claims.JTI)
	}
	_ = store.RevokeUserSessions(r.Context(), s.db.App, tenant, userID)
	s.audit(r, "user.change_password", "user", userID, nil)
	writeJSON(w, http.StatusOK, map[string]any{"status": "password_changed", "sessions_revoked": true})
}

type resetPasswordRequest struct {
	NewPassword string `json:"new_password"`
}

// handleResetPassword lets an ADMIN set a user's password (e.g. forgot it). All
// of that user's sessions are revoked so the reset takes effect immediately.
func (s *Server) handleResetPassword(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	targetID := r.PathValue("id")
	var req resetPasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.NewPassword == "" {
		writeError(w, http.StatusBadRequest, "new_password is required")
		return
	}
	if _, err := store.GetUser(r.Context(), s.db.App, tenant, targetID); err != nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		s.log.Error("hash password", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to reset password")
		return
	}
	if err := store.SetUserPassword(r.Context(), s.db.App, tenant, targetID, string(hash)); err != nil {
		s.log.Error("reset user password", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to reset password")
		return
	}
	if err := store.RevokeUserSessions(r.Context(), s.db.App, tenant, targetID); err != nil {
		s.log.Error("revoke sessions on reset", "error", err)
	}
	s.audit(r, "user.reset_password", "user", targetID, nil)
	writeJSON(w, http.StatusOK, map[string]any{"status": "password_reset", "sessions_revoked": true})
}

type changeRoleRequest struct {
	Role string `json:"role"`
}

// handleChangeRole lets an ADMIN change a user's role, subject to not granting
// a role above the caller's own.
func (s *Server) handleChangeRole(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	targetID := r.PathValue("id")
	var req changeRoleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Role == "" {
		writeError(w, http.StatusBadRequest, "role is required")
		return
	}
	rank, ok := roleRank[req.Role]
	if !ok {
		writeError(w, http.StatusBadRequest, "role must be ADMIN, OPERATOR, DEVELOPER, or VIEWER")
		return
	}
	if rank > roleRank[roleFrom(r)] {
		writeError(w, http.StatusForbidden, "cannot grant a role above your own")
		return
	}
	target, err := store.GetUser(r.Context(), s.db.App, tenant, targetID)
	if err != nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	oldRole := target.Role
	if err := store.SetUserRole(r.Context(), s.db.App, tenant, targetID, req.Role); err != nil {
		s.log.Error("set user role", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to change role")
		return
	}
	s.audit(r, "user.change_role", "user", targetID, map[string]any{"from": oldRole, "to": req.Role})
	writeJSON(w, http.StatusOK, map[string]any{"status": "role_changed", "id": targetID, "role": req.Role})
}
