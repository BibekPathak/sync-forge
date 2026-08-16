package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"

	"syncforge/internal/store"
)

// ctxKey is an unexported type to avoid collisions.
type ctxKey string

const (
	ctxTenantID ctxKey = "tenant_id"
	ctxRole     ctxKey = "role"
	ctxActor    ctxKey = "actor"
	ctxKeyID    ctxKey = "key_id"
)

// roleRank orders the tenant roles: a higher role implies every capability of
// the lower ones, so an endpoint guarded by requireRole(min) is accessible to
// any key whose rank is at least min's.
var roleRank = map[string]int{
	"VIEWER":    0,
	"DEVELOPER": 1,
	"OPERATOR":  2,
	"ADMIN":     3,
}

// hashAPIKey hashes an API key for storage. We store only the hash; the raw
// key is shown once at creation time.
func hashAPIKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// actorFromRequest extracts an API key from the Authorization bearer header or
// the X-API-Key header.
func actorFromRequest(r *http.Request) string {
	if h := r.Header.Get("Authorization"); len(h) > 7 && h[:7] == "Bearer " {
		return h[7:]
	}
	if h := r.Header.Get("X-API-Key"); h != "" {
		return h
	}
	return ""
}

// requireAPIKey authenticates a request via a tenant API key and injects the
// tenant context into the request. Uses the admin pool to resolve the key
// before a tenant context exists.
func (s *Server) requireAPIKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := actorFromRequest(r)
		if key == "" {
			writeError(w, http.StatusUnauthorized, "missing api key")
			return
		}
		k, err := store.VerifyAPIKey(r.Context(), s.db.Admin, hashAPIKey(key))
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusUnauthorized, "invalid api key")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "auth backend error")
			return
		}
		ctx := context.WithValue(r.Context(), ctxTenantID, k.TenantID)
		ctx = context.WithValue(ctx, ctxRole, k.Role)
		ctx = context.WithValue(ctx, ctxActor, "key:"+k.Name)
		ctx = context.WithValue(ctx, ctxKeyID, k.ID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requireRole authenticates (API key or user session token) and then enforces
// a minimum role. Role ranking lives in roleRank; ADMIN (3) > OPERATOR (2) >
// DEVELOPER (1) > VIEWER (0). An endpoint requiring role R rejects any caller
// with a lower rank.
func (s *Server) requireRole(minRole string) func(http.Handler) http.Handler {
	minRank, ok := roleRank[minRole]
	if !ok {
		panic("requireRole: unknown role " + minRole)
	}
	return func(next http.Handler) http.Handler {
		return s.authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			role := roleFrom(r)
			if roleRank[role] < minRank {
				writeError(w, http.StatusForbidden, "insufficient role")
				return
			}
			next.ServeHTTP(w, r)
		}))
	}
}

// tenantIDFrom returns the tenant id injected by requireAPIKey.
func tenantIDFrom(r *http.Request) string {
	v, _ := r.Context().Value(ctxTenantID).(string)
	return v
}

// roleFrom returns the role injected by requireAPIKey.
func roleFrom(r *http.Request) string {
	v, _ := r.Context().Value(ctxRole).(string)
	return v
}

// actorKeyID returns the id of the API key authenticating the request.
func actorKeyID(r *http.Request) string {
	v, _ := r.Context().Value(ctxKeyID).(string)
	return v
}
