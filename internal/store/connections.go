package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// Connection is a tenant-owned connector registration.
type Connection struct {
	ID              string         `json:"id"`
	TenantID        string         `json:"tenant_id"`
	Name            string         `json:"name"`
	Provider        string         `json:"provider"`
	BaseURL         string         `json:"base_url"`
	Status          string         `json:"status"`
	WebhookSecret   string         `json:"webhook_secret"`
	Config          map[string]any `json:"config"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	LastHealthCheck *time.Time     `json:"last_health_check"`
}

func CreateConnection(ctx context.Context, pool *pgxpool.Pool, c Connection) (Connection, error) {
	if c.Config == nil {
		c.Config = map[string]any{}
	}
	var out Connection
	_, err := db.WithTenant[struct{}](ctx, pool, c.TenantID, func(tx pgx.Tx) (struct{}, error) {
		return struct{}{}, tx.QueryRow(ctx,
			`INSERT INTO connections (tenant_id, name, provider, base_url, status, webhook_secret, config)
			 VALUES ($1,$2,$3,$4,$5,$6,$7)
			 RETURNING id, tenant_id, name, provider, base_url, status, webhook_secret, config, created_at, updated_at`,
			c.TenantID, c.Name, c.Provider, c.BaseURL, c.Status, c.WebhookSecret, c.Config,
		).Scan(&out.ID, &out.TenantID, &out.Name, &out.Provider, &out.BaseURL, &out.Status,
			&out.WebhookSecret, &out.Config, &out.CreatedAt, &out.UpdatedAt)
	})
	return out, err
}

func ListConnections(ctx context.Context, pool *pgxpool.Pool, tenantID string) ([]Connection, error) {
	out, err := db.WithTenant[[]Connection](ctx, pool, tenantID, func(tx pgx.Tx) ([]Connection, error) {
		rows, err := tx.Query(ctx,
			`SELECT id, tenant_id, name, provider, base_url, status, webhook_secret, config, created_at, updated_at
			 FROM connections ORDER BY created_at`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []Connection
		for rows.Next() {
			var c Connection
			if err := rows.Scan(&c.ID, &c.TenantID, &c.Name, &c.Provider, &c.BaseURL, &c.Status,
				&c.WebhookSecret, &c.Config, &c.CreatedAt, &c.UpdatedAt); err != nil {
				return nil, err
			}
			res = append(res, c)
		}
		return res, rows.Err()
	})
	return out, err
}

func GetConnection(ctx context.Context, pool *pgxpool.Pool, tenantID, id string) (Connection, error) {
	out, err := db.WithTenant[Connection](ctx, pool, tenantID, func(tx pgx.Tx) (Connection, error) {
		var c Connection
		err := tx.QueryRow(ctx,
			`SELECT id, tenant_id, name, provider, base_url, status, webhook_secret, config, created_at, updated_at
			 FROM connections WHERE id=$1`, id,
		).Scan(&c.ID, &c.TenantID, &c.Name, &c.Provider, &c.BaseURL, &c.Status,
			&c.WebhookSecret, &c.Config, &c.CreatedAt, &c.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return Connection{}, ErrNotFound
		}
		return c, err
	})
	return out, err
}

// GetConnectionByProvider resolves the webhook target connection for a tenant
// and provider (webhook routes by provider, not by generated id).
func GetConnectionByProvider(ctx context.Context, pool *pgxpool.Pool, tenantID, provider string) (Connection, error) {
	out, err := db.WithTenant[Connection](ctx, pool, tenantID, func(tx pgx.Tx) (Connection, error) {
		var c Connection
		err := tx.QueryRow(ctx,
			`SELECT id, tenant_id, name, provider, base_url, status, webhook_secret, config, created_at, updated_at
			 FROM connections WHERE provider=$1 LIMIT 1`, provider,
		).Scan(&c.ID, &c.TenantID, &c.Name, &c.Provider, &c.BaseURL, &c.Status,
			&c.WebhookSecret, &c.Config, &c.CreatedAt, &c.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return Connection{}, ErrNotFound
		}
		return c, err
	})
	return out, err
}

// SetConnectionStatus updates health/status of a connection.
func SetConnectionStatus(ctx context.Context, pool *pgxpool.Pool, tenantID, id, status string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE connections SET status=$1, last_health_check=now(), updated_at=now() WHERE id=$2`,
			status, id)
		return err
	})
}
