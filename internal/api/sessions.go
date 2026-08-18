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

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"syncforge/internal/store"
	"syncforge/internal/totp"
)

// sessionTTL bounds how long a user login token is valid.
const sessionTTL = 12 * time.Hour

// sessionClaims is the signed payload of a user session token. JTI links the
// token to a live row in the sessions table so it can be revoked.
type sessionClaims struct {
	JTI      string `json:"jti"`
	UserID   string `json:"uid"`
	TenantID string `json:"tid"`
	Role     string `json:"role"`
	Exp      int64  `json:"exp"`
}

// mintSessionToken persists a server-side session row and signs a token that
// references it. The session must exist for the token to authenticate, so
// logout and revocation take effect immediately.
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

// verifySessionToken validates a session token's signature, expiry, and that a
// live server-side session row exists (not revoked). Signature checks run first
// so a forged token never touches the DB.
func (s *Server) verifySessionToken(r *http.Request, token string) (sessionClaims, error) {
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
	if c.JTI == "" || c.UserID == "" || c.TenantID == "" || c.Role == "" {
		return sessionClaims{}, errors.New("incomplete token claims")
	}
	if c.Exp < time.Now().Unix() {
		return sessionClaims{}, errors.New("token expired")
	}
	if _, err := store.GetLiveSession(r.Context(), s.db.App, c.TenantID, c.JTI); err != nil {
		return sessionClaims{}, errors.New("session revoked or expired")
	}
	return c, nil
}

type loginRequest struct {
	TenantSlug string `json:"tenant_slug"`
	Email      string `json:"email"`
	Password   string `json:"password"`
	Code       string `json:"code"`
}

// handleLogin authenticates a user by email + password and returns a signed
// session token. The tenant is resolved by slug (users are tenant-scoped) via
// the BYPASSRLS admin pool, mirroring API-key verification. It enforces a
// per-IP throttle (Redis, best-effort) and an account lockout after repeated
// failures.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TenantSlug == "" || req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "tenant_slug, email and password are required")
		return
	}

	// Per-IP throttle: too many login attempts from one address, fast.
	if s.loginThrottled(r) {
		writeError(w, http.StatusTooManyRequests, "too many login attempts, try again later")
		return
	}

	tenant, err := store.GetTenantBySlug(r.Context(), s.db.Admin, req.TenantSlug)
	if err != nil {
		s.loginFailure(r, "", req.TenantSlug, req.Email, "unknown_tenant")
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	// Account lockout: repeated failures within the window block further
	// attempts until the window passes, regardless of the supplied password.
	window := time.Duration(s.cfg.LoginLockoutMin) * time.Minute
	failures, cerr := store.CountRecentFailures(r.Context(), s.db.Admin, tenant.ID, req.Email, window)
	if cerr == nil && failures >= s.cfg.LoginMaxFailures {
		s.auditLogin(r, tenant.ID, "auth.login_failed", req.Email, map[string]any{"tenant_slug": req.TenantSlug, "reason": "account_locked"})
		writeError(w, http.StatusTooManyRequests, "account temporarily locked after too many failed attempts")
		return
	}

	user, err := store.GetUserByEmail(r.Context(), s.db.Admin, tenant.ID, req.Email)
	if err != nil {
		s.loginFailure(r, tenant.ID, req.TenantSlug, req.Email, "unknown_user")
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		s.loginFailure(r, tenant.ID, req.TenantSlug, req.Email, "bad_password")
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	// Multi-factor: when the user has TOTP enabled, a valid 6-digit code is
	// required in addition to the password. Either the live TOTP code or a
	// single-use backup code (consumed on success) is accepted.
	if user.TOTPEnabled {
		if req.Code == "" {
			s.loginFailure(r, tenant.ID, req.TenantSlug, req.Email, "mfa_code_required")
			writeError(w, http.StatusUnauthorized, "mfa code required")
			return
		}
		ok, err := totp.Validate(user.TOTPSecret, req.Code, nowFunc())
		if err == nil && ok {
			// Authenticator code: nothing further to consume.
		} else if consumed, cerr := store.ConsumeBackupCode(r.Context(), s.db.Admin, user.TenantID, user.ID, backupCodeHash(req.Code)); cerr != nil || !consumed {
			s.loginFailure(r, tenant.ID, req.TenantSlug, req.Email, "bad_mfa_code")
			writeError(w, http.StatusUnauthorized, "invalid mfa code")
			return
		}
	}

	// Successful login: clear the failure history so the lockout counter starts
	// fresh, then record the session server-side.
	_ = store.ClearLoginFailures(r.Context(), s.db.Admin, user.TenantID, req.Email)
	sess, err := store.CreateSession(r.Context(), s.db.Admin, store.Session{
		JTI:       uuid.NewString(),
		UserID:    user.ID,
		TenantID:  user.TenantID,
		Role:      user.Role,
		ExpiresAt: time.Now().Add(sessionTTL).UTC(),
	})
	if err != nil {
		s.log.Error("create session", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to issue session")
		return
	}
	token, err := s.mintSessionToken(sessionClaims{JTI: sess.JTI, UserID: user.ID, TenantID: user.TenantID, Role: user.Role})
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

// handleLogout revokes the caller's current session so its token can no longer
// authenticate.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	token := bearerTokenFrom(r)
	c, err := s.verifySessionToken(r, token)
	if err != nil {
		// Already invalid: treat as a successful logout.
		writeJSON(w, http.StatusOK, map[string]any{"status": "logged_out"})
		return
	}
	if err := store.RevokeSession(r.Context(), s.db.App, c.TenantID, c.JTI); err != nil {
		s.log.Error("revoke session on logout", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to log out")
		return
	}
	s.auditLogin(r, c.TenantID, "auth.logout", "user:"+c.UserID, nil)
	writeJSON(w, http.StatusOK, map[string]any{"status": "logged_out"})
}

// handleRefresh rotates the caller's session: the current one is revoked and a
// fresh token is issued, so a leaked token cannot be replayed forever.
func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	token := bearerTokenFrom(r)
	c, err := s.verifySessionToken(r, token)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid or expired session token")
		return
	}
	if err := store.RevokeSession(r.Context(), s.db.App, c.TenantID, c.JTI); err != nil {
		s.log.Error("revoke session on refresh", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to refresh session")
		return
	}
	sess, err := store.CreateSession(r.Context(), s.db.Admin, store.Session{
		JTI:       uuid.NewString(),
		UserID:    c.UserID,
		TenantID:  c.TenantID,
		Role:      c.Role,
		ExpiresAt: time.Now().Add(sessionTTL).UTC(),
	})
	if err != nil {
		s.log.Error("create session on refresh", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to refresh session")
		return
	}
	newToken, err := s.mintSessionToken(sessionClaims{JTI: sess.JTI, UserID: c.UserID, TenantID: c.TenantID, Role: c.Role})
	if err != nil {
		s.log.Error("mint refresh token", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to refresh session")
		return
	}
	s.auditLogin(r, c.TenantID, "auth.refresh", "user:"+c.UserID, nil)
	writeJSON(w, http.StatusOK, map[string]any{
		"token":      newToken,
		"token_type": "Bearer",
		"expires_in": int(sessionTTL.Seconds()),
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
		c, err := s.verifySessionToken(r, token)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid or expired session token")
			return
		}
		ctx := context.WithValue(r.Context(), ctxTenantID, c.TenantID)
		ctx = context.WithValue(ctx, ctxRole, c.Role)
		ctx = context.WithValue(ctx, ctxActor, "user:"+c.UserID)
		ctx = context.WithValue(ctx, ctxUserID, c.UserID)
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

// clientIP returns the remote address (used for the per-IP throttle).
func clientIP(r *http.Request) string {
	return r.RemoteAddr
}

// loginThrottled applies a per-IP fixed-window limit on login attempts using
// Redis. Best-effort: if Redis is unavailable the request is allowed through
// rather than locking everyone out.
func (s *Server) loginThrottled(r *http.Request) bool {
	if s.cfg.LoginThrottlePerMin <= 0 || s.cache == nil {
		return false
	}
	key := "login:throttle:" + clientIP(r)
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	n, err := s.cache.Client.Incr(ctx, key).Result()
	if err != nil {
		s.log.Warn("login throttle backend unavailable, allowing", "error", err)
		return false
	}
	if n == 1 {
		_ = s.cache.Client.Expire(ctx, key, time.Minute).Err()
	}
	return n > int64(s.cfg.LoginThrottlePerMin)
}

// loginFailure records a failed attempt (drives lockout) and audits it.
func (s *Server) loginFailure(r *http.Request, tenantID, tenantSlug, email, reason string) {
	pool := s.db.Admin
	if tenantID != "" {
		if err := store.RecordLoginAttempt(r.Context(), pool, store.LoginAttempt{
			TenantID: tenantID,
			Email:    email,
			IP:       clientIP(r),
			Success:  false,
		}); err != nil {
			s.log.Warn("record login failure", "email", email, "error", err)
		}
	}
	s.auditLogin(r, tenantID, "auth.login_failed", email, map[string]any{"tenant_slug": tenantSlug, "reason": reason})
}
