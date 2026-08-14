package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// APIKey is a tenant API key used for service-to-service authentication.
// Verification happens on the admin pool because the key must be resolved
// before a tenant context exists.
type APIKey struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	Name      string    `json:"name"`
	Role      string    `json:"role"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

func CreateAPIKey(ctx context.Context, pool *pgxpool.Pool, tenantID, name, role, keyHash string) (APIKey, error) {
	var k APIKey
	err := pool.QueryRow(ctx,
		`INSERT INTO api_keys (tenant_id, name, role, key_hash)
		 VALUES ($1,$2,$3,$4)
		 RETURNING id, tenant_id, name, role, enabled, created_at`,
		tenantID, name, role, keyHash,
	).Scan(&k.ID, &k.TenantID, &k.Name, &k.Role, &k.Enabled, &k.CreatedAt)
	return k, err
}

// VerifyAPIKey looks up an API key by hash (admin pool, no tenant context
// required). Returns ErrNotFound when absent/disabled.
func VerifyAPIKey(ctx context.Context, pool *pgxpool.Pool, keyHash string) (APIKey, error) {
	var k APIKey
	err := pool.QueryRow(ctx,
		`SELECT id, tenant_id, name, role, enabled, created_at
		 FROM api_keys WHERE key_hash=$1 AND enabled=true`, keyHash,
	).Scan(&k.ID, &k.TenantID, &k.Name, &k.Role, &k.Enabled, &k.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return APIKey{}, ErrNotFound
	}
	if err != nil {
		return APIKey{}, err
	}
	TouchKey(ctx, pool, k.ID)
	return k, nil
}

// TouchKey records last_used_at (best-effort).
func TouchKey(ctx context.Context, pool *pgxpool.Pool, keyID string) {
	_, _ = pool.Exec(ctx, `UPDATE api_keys SET last_used_at=now() WHERE id=$1`, keyID)
}
