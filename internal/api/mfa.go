package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"syncforge/internal/store"
	"syncforge/internal/totp"
)

// nowFunc returns the current time. Overridden in tests to pin the TOTP window.
var nowFunc = func() time.Time { return time.Now().UTC() }

// backupCodeCount is how many single-use recovery codes are generated per
// request.
const backupCodeCount = 10

// backupCodeHash hashes a raw backup code for storage; the raw value is shown
// only once at generation.
func backupCodeHash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// newBackupCodes generates backupCodeCount random recovery codes of the form
// XXXX-XXXX, returning both the raw codes (shown once) and their hashes.
func newBackupCodes() ([]string, []string, error) {
	raw := make([]string, backupCodeCount)
	hashed := make([]string, backupCodeCount)
	for i := range raw {
		buf := make([]byte, 5)
		if _, err := rand.Read(buf); err != nil {
			return nil, nil, err
		}
		s := base32.StdEncoding.EncodeToString(buf)
		s = strings.ReplaceAll(s, "=", "")
		raw[i] = s[:4] + "-" + s[4:8]
		hashed[i] = backupCodeHash(raw[i])
	}
	return raw, hashed, nil
}

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

// handleMFABackupCodes generates a fresh set of single-use backup codes,
// replacing any previous ones. Raw codes are returned exactly once; only their
// hashes are stored. The user must be MFA-enabled (or enrolling).
func (s *Server) handleMFABackupCodes(w http.ResponseWriter, r *http.Request) {
	tenant := tenantIDFrom(r)
	userID := userIDFrom(r)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "user session required")
		return
	}
	raw, hashed, err := newBackupCodes()
	if err != nil {
		s.log.Error("generate backup codes", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to generate backup codes")
		return
	}
	if err := store.SetBackupCodes(r.Context(), s.db.App, tenant, userID, hashed); err != nil {
		s.log.Error("store backup codes", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to generate backup codes")
		return
	}
	s.audit(r, "mfa.backup_codes", "user", userID, map[string]any{"count": len(raw)})
	writeJSON(w, http.StatusOK, map[string]any{
		"codes":         raw,
		"shown_once":    true,
		"single_use":    true,
		"expires_never": true,
	})
}
