package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"syncforge/internal/store"
)

// sessionTTL bounds how long a user login token is valid.
const sessionTTL = 12 * time.Hour

// sessionClaims is the signed payload of a user session token.
type sessionClaims struct {
	UserID   string `json:"uid"`
	TenantID string `json:"tid"`
	Role     string `json:"role"`
	Exp      int64  `json:"exp"`
}

// mintSessionToken signs the user's identity so requireUser can authenticate
// requests without a DB round-trip. Format: base64url(claims).base64url(hmac).
func (s *Server) mintSessionToken(u sessionClaims) (string, error) {
	u.Exp = time.Now().Add(sessionTTL).Unix()
	claims, err := json.Marshal(u)
	if err != nil {
		return "", err
	}
	b64 := base64.RawURLEncoding.EncodeToString(claims)
	mac := hmac.New(sha256.New, []byte(s.cfg.AuthSecret))
	mac.Write([]byte(b64))
	return b64 + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// verifySessionToken validates a session token's signature and expiry.
func (s *Server) verifySessionToken(token string) (sessionClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return sessionClaims{}, errors.New("malformed token")
	}
	claimsRaw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return sessionClaims{}, errors.New("malformed token")
	}
	mac := hmac.New(sha256.New, []byte(s.cfg.AuthSecret))
	mac.Write([]byte(parts[0]))
	want, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || !hmac.Equal(want, mac.Sum(nil)) {
		return sessionClaims{}, errors.New("invalid token signature")
	}
	var c sessionClaims
	if err := json.Unmarshal(claimsRaw, &c); err != nil {
		return sessionClaims{}, errors.New("invalid token claims")
	}
	if c.UserID == "" || c.TenantID == "" || c.Role == "" {
		return sessionClaims{}, errors.New("incomplete token claims")
	}
	if c.Exp < time.Now().Unix() {
		return sessionClaims{}, errors.New("token expired")
	}
	return c, nil
}

type loginRequest struct {
	TenantSlug string `json:"tenant_slug"`
	Email      string `json:"email"`
	Password   string `json:"password"`
}

// handleLogin authenticates a user by email + password and returns a signed
// session token. The tenant is resolved by slug (users are tenant-scoped) via
// the BYPASSRLS admin pool, mirroring API-key verification.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TenantSlug == "" || req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "tenant_slug, email and password are required")
		return
	}
	tenant, err := store.GetTenantBySlug(r.Context(), s.db.Admin, req.TenantSlug)
	if err != nil {
		s.auditLogin(r, "", "auth.login_failed", req.Email, map[string]any{"tenant_slug": req.TenantSlug, "reason": "unknown_tenant"})
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	user, err := store.GetUserByEmail(r.Context(), s.db.Admin, tenant.ID, req.Email)
	if err != nil {
		s.auditLogin(r, tenant.ID, "auth.login_failed", req.Email, map[string]any{"tenant_slug": req.TenantSlug, "reason": "unknown_user"})
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		s.auditLogin(r, tenant.ID, "auth.login_failed", req.Email, map[string]any{"tenant_slug": req.TenantSlug, "reason": "bad_password"})
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	token, err := s.mintSessionToken(sessionClaims{UserID: user.ID, TenantID: user.TenantID, Role: user.Role})
	if err != nil {
		s.log.Error("mint session token", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to issue session")
		return
	}
	s.auditLogin(r, user.TenantID, "auth.login", user.Email, map[string]any{"role": user.Role})
	writeJSON(w, http.StatusOK, map[string]any{
		"token":      token,
		"token_type": "Bearer",
		"expires_in": int(sessionTTL.Seconds()),
		"user": map[string]any{
			"id": user.ID, "email": user.Email, "role": user.Role,
		},
	})
}

// bearerTokenFrom extracts a Bearer token from the Authorization header.
func bearerTokenFrom(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
		return h[7:]
	}
	return ""
}

// requireUser authenticates via a signed user session token. API-key auth is
// unchanged; this middleware is for the human-facing login path.
func (s *Server) requireUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := bearerTokenFrom(r)
		if token == "" {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		c, err := s.verifySessionToken(token)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid or expired session token")
			return
		}
		ctx := context.WithValue(r.Context(), ctxTenantID, c.TenantID)
		ctx = context.WithValue(ctx, ctxRole, c.Role)
		ctx = context.WithValue(ctx, ctxActor, "user:"+c.UserID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authenticate first tries API-key auth, then falls back to a user session
// token. Used by requireRole so both credential types reach the same role gate.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// If a session token is present, prefer the user path (its issuer is
		// bound to a tenant; API keys remain valid side-by-side).
		if bearerTokenFrom(r) != "" {
			s.requireUser(next).ServeHTTP(w, r)
			return
		}
		s.requireAPIKey(next).ServeHTTP(w, r)
	})
}

// audit records an operator/security event into the tenant's audit log. It is
// best-effort: audit failures must never fail the underlying operation.
func (s *Server) audit(r *http.Request, action, resource, resourceID string, meta map[string]any) {
	tenant := tenantIDFrom(r)
	actor := "unknown"
	if v, _ := r.Context().Value(ctxActor).(string); v != "" {
		actor = v
	}
	_, err := store.InsertAuditLog(r.Context(), s.db.App, tenant, store.AuditLog{
		TenantID:   &tenant,
		Actor:      actor,
		Action:     action,
		Resource:   resource,
		ResourceID: resourceID,
		Metadata:   meta,
	})
	if err != nil {
		s.log.Warn("audit log write failed", "action", action, "resource", resource, "error", err)
	}
}

// auditLogin records authentication events (success and failure). Failures use
// the admin pool with an optional tenant so attempts against unknown tenants
// are still captured.
func (s *Server) auditLogin(r *http.Request, tenantID, action, email string, meta map[string]any) {
	pool := s.db.App
	var tenantPtr *string
	if tenantID == "" {
		pool = s.db.Admin
	} else {
		tenantPtr = &tenantID
	}
	_, err := store.InsertAuditLog(r.Context(), pool, tenantID, store.AuditLog{
		TenantID: tenantPtr,
		Actor:    email,
		Action:   action,
		Resource: "auth",
		Metadata: meta,
	})
	if err != nil {
		s.log.Warn("audit log write failed", "action", action, "error", err)
	}
}
