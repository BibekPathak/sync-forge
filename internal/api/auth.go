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
)

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
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requireBootstrap guards tenant-management endpoints. Until full RBAC lands
// (Phase 7), these use a fixed bootstrap key from configuration.
func (s *Server) requireBootstrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Bootstrap-Key") != s.cfg.BootstrapKey {
			writeError(w, http.StatusUnauthorized, "invalid bootstrap key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// tenantIDFrom returns the tenant id injected by requireAPIKey.
func tenantIDFrom(r *http.Request) string {
	v, _ := r.Context().Value(ctxTenantID).(string)
	return v
}
