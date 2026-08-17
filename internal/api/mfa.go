package api

import (
	"encoding/json"
	"net/http"
	"time"

	"syncforge/internal/store"
	"syncforge/internal/totp"
)

// nowFunc returns the current time. Overridden in tests to pin the TOTP window.
var nowFunc = func() time.Time { return time.Now().UTC() }

// handleMFAEnroll generates a fresh TOTP secret for the calling user (self
// service) and returns its provisioning URI. The secret is stored but not yet
// enabled; the user confirms with a code before login enforces it.
func (s *Server) handleMFAEnroll(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	userID := userIDFrom(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "user session required")
		return
	}
	secret, err := totp.NewSecret()
	if err != nil {
		s.log.Error("generate totp secret", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to enroll mfa")
		return
	}
	if err := store.SetTOTPSecret(r.Context(), s.db.App, tenant, userID, secret); err != nil {
		s.log.Error("store totp secret", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to enroll mfa")
		return
	}
	// Disable while pending confirmation so an aborted enroll does not lock the
	// user out before they scan the QR code.
	if err := store.SetTOTPEnabled(r.Context(), s.db.App, tenant, userID, false); err != nil {
		s.log.Error("reset totp enabled", "error", err)
	}
	email := "user:" + userID
	if v, _ := r.Context().Value(ctxActor).(string); v != "" {
		email = v
	}
	s.audit(r, "mfa.enroll", "user", userID, nil)
	writeJSON(w, http.StatusOK, map[string]any{
		"secret":    secret,
		"otpauth":   totp.URI("SyncForge", email, secret),
		"enabled":   false,
		"confirmed": false,
	})
}

type mfaConfirmRequest struct {
	Code string `json:"code"`
}

// handleMFAConfirm verifies a code against the pending secret and enables MFA
// for the user.
func (s *Server) handleMFAConfirm(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	userID := userIDFrom(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "user session required")
		return
	}
	var req mfaConfirmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Code == "" {
		writeError(w, http.StatusBadRequest, "code is required")
		return
	}
	secret, err := store.GetUserTOTPSecret(r.Context(), s.db.App, tenant, userID)
	if err != nil || secret == "" {
		writeError(w, http.StatusBadRequest, "mfa not enrolled")
		return
	}
	ok, err := totp.Validate(secret, req.Code, nowFunc())
	if err != nil {
		s.log.Error("validate totp code", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to confirm mfa")
		return
	}
	if !ok {
		s.audit(r, "mfa.confirm_failed", "user", userID, nil)
		writeError(w, http.StatusUnauthorized, "invalid code")
		return
	}
	if err := store.SetTOTPEnabled(r.Context(), s.db.App, tenant, userID, true); err != nil {
		s.log.Error("enable totp", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to confirm mfa")
		return
	}
	s.audit(r, "mfa.confirm", "user", userID, nil)
	writeJSON(w, http.StatusOK, map[string]any{"enabled": true, "confirmed": true})
}

// handleMFADisable turns MFA off for the calling user after verifying a code.
func (s *Server) handleMFADisable(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	userID := userIDFrom(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "user session required")
		return
	}
	var req mfaConfirmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Code == "" {
		writeError(w, http.StatusBadRequest, "code is required")
		return
	}
	secret, err := store.GetUserTOTPSecret(r.Context(), s.db.App, tenant, userID)
	if err != nil || secret == "" {
		writeError(w, http.StatusBadRequest, "mfa not enrolled")
		return
	}
	ok, err := totp.Validate(secret, req.Code, nowFunc())
	if err != nil {
		s.log.Error("validate totp code", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to disable mfa")
		return
	}
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid code")
		return
	}
	if err := store.SetTOTPEnabled(r.Context(), s.db.App, tenant, userID, false); err != nil {
		s.log.Error("disable totp", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to disable mfa")
		return
	}
	s.audit(r, "mfa.disable", "user", userID, nil)
	writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
}
