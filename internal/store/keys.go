package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
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

// CreateTenantAPIKey creates an API key inside the caller's tenant context so
// Row-Level Security enforces the WITH CHECK on tenant_id. Unlike
// CreateAPIKey (admin pool, cross-tenant provisioning), this is the path used
// by tenant-scoped key management.
func CreateTenantAPIKey(ctx context.Context, pool *pgxpool.Pool, tenantID, name, role, keyHash string) (APIKey, error) {
	var k APIKey
	_, err := db.WithTenant[struct{}](ctx, pool, tenantID, func(tx pgx.Tx) (struct{}, error) {
		err := tx.QueryRow(ctx,
			`INSERT INTO api_keys (tenant_id, name, role, key_hash)
			 VALUES ($1,$2,$3,$4)
			 RETURNING id, tenant_id, name, role, enabled, created_at`,
			tenantID, name, role, keyHash,
		).Scan(&k.ID, &k.TenantID, &k.Name, &k.Role, &k.Enabled, &k.CreatedAt)
		return struct{}{}, err
	})
	return k, err
}

// ListAPIKeys lists the tenant's API keys (no hashes; raw keys are shown once
// at creation).
func ListAPIKeys(ctx context.Context, pool *pgxpool.Pool, tenantID string) ([]APIKey, error) {
	out, err := db.WithTenant[[]APIKey](ctx, pool, tenantID, func(tx pgx.Tx) ([]APIKey, error) {
		rows, err := tx.Query(ctx,
			`SELECT id, tenant_id, name, role, enabled, created_at
			 FROM api_keys ORDER BY created_at`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []APIKey
		for rows.Next() {
			var k APIKey
			if err := rows.Scan(&k.ID, &k.TenantID, &k.Name, &k.Role, &k.Enabled, &k.CreatedAt); err != nil {
				return nil, err
			}
			res = append(res, k)
		}
		return res, rows.Err()
	})
	return out, err
}

// RevokeAPIKey disables an API key in the caller's tenant. Returns ErrNotFound
// when the key does not belong to the tenant.
func RevokeAPIKey(ctx context.Context, pool *pgxpool.Pool, tenantID, id string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE api_keys SET enabled=false WHERE id=$1 AND tenant_id=$2`, id, tenantID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}
