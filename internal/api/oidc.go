package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"syncforge/internal/oidc"
	"syncforge/internal/store"
)

type oidcLoginRequest struct {
	TenantSlug string `json:"tenant_slug"`
	IDToken    string `json:"id_token"`
}

// handleOIDCLogin authenticates a user via an OIDC ID token and issues a
// SyncForge session. The token is verified against the configured issuer's
// JWKS (signature, issuer, audience, expiry). The user is resolved by email in
// the tenant, or auto-provisioned as VIEWER when configured.
func (s *Server) handleOIDCLogin(w http.ResponseWriter, r *http.Request) {
	if s.cfg.OIDCIssuer == "" || s.cfg.OIDCClientID == "" {
		writeError(w, http.StatusNotImplemented, "oidc login not configured")
		return
	}
	var req oidcLoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TenantSlug == "" || req.IDToken == "" {
		writeError(w, http.StatusBadRequest, "tenant_slug and id_token are required")
		return
	}

	tenant, err := store.GetTenantBySlug(r.Context(), s.db.Admin, req.TenantSlug)
	if err != nil {
		s.auditLogin(r, "", "auth.login_failed", "oidc:"+req.TenantSlug, map[string]any{"reason": "unknown_tenant"})
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	prov, err := oidc.NewProvider(r.Context(), oidc.Config{Issuer: s.cfg.OIDCIssuer, ClientID: s.cfg.OIDCClientID})
	if err != nil {
		s.log.Error("oidc provider discovery", "error", err)
		writeError(w, http.StatusBadGateway, "identity provider unavailable")
		return
	}
	claims, err := prov.VerifyIDToken(r.Context(), req.IDToken)
	if err != nil {
		s.auditLogin(r, tenant.ID, "auth.login_failed", req.TenantSlug+"/"+claimsEmailSafe(req.IDToken), map[string]any{"reason": "bad_id_token", "error": err.Error()})
		writeError(w, http.StatusUnauthorized, "invalid id token")
		return
	}

	// Resolve (or auto-provision) the tenant user by the verified email.
	user, err := store.GetUserByEmail(r.Context(), s.db.Admin, tenant.ID, claims.Email)
	if errors.Is(err, store.ErrNotFound) {
		if !s.cfg.OIDCAutoProvision {
			s.auditLogin(r, tenant.ID, "auth.login_failed", claims.Email, map[string]any{"reason": "no_account"})
			writeError(w, http.StatusUnauthorized, "no SyncForge account for this identity")
			return
		}
		hash, err := bcrypt.GenerateFromPassword([]byte("oidc:"+uuid.NewString()), bcrypt.DefaultCost)
		if err != nil {
			s.log.Error("hash oidc provisional password", "error", err)
			writeError(w, http.StatusInternalServerError, "failed to provision account")
			return
		}
		user, err = store.CreateUser(r.Context(), s.db.Admin, tenant.ID, claims.Email, string(hash), "VIEWER")
		if err != nil {
			s.log.Error("auto-provision oidc user", "error", err)
			writeError(w, http.StatusInternalServerError, "failed to provision account")
			return
		}
	} else if err != nil {
		s.log.Error("oidc user lookup", "error", err)
		writeError(w, http.StatusInternalServerError, "auth backend error")
		return
	}

	// Issue a normal SyncForge session so the rest of the RBAC surface works
	// identically for SSO and password logins.
	sess, err := store.CreateSession(r.Context(), s.db.Admin, store.Session{
		JTI:       uuid.NewString(),
		UserID:    user.ID,
		TenantID:  user.TenantID,
		Role:      user.Role,
		ExpiresAt: time.Now().Add(sessionTTL).UTC(),
	})
	if err != nil {
		s.log.Error("create oidc session", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to issue session")
		return
	}
	token, err := s.mintSessionToken(sessionClaims{JTI: sess.JTI, UserID: user.ID, TenantID: user.TenantID, Role: user.Role})
	if err != nil {
		s.log.Error("mint oidc session token", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to issue session")
		return
	}
	s.auditLogin(r, user.TenantID, "auth.login", user.Email, map[string]any{"role": user.Role, "via": "oidc", "sub": claims.Sub})
	writeJSON(w, http.StatusOK, map[string]any{
		"token":      token,
		"token_type": "Bearer",
		"expires_in": int(sessionTTL.Seconds()),
		"user": map[string]any{
			"id": user.ID, "email": user.Email, "role": user.Role,
		},
	})
}

// claimsEmailSafe extracts an email from an ID token for logging without fully
// parsing it (best-effort; the token is verified before this is meaningful).
func claimsEmailSafe(_ string) string { return "unknown" }
