package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Tenant is a tenant row. Tenant management is cross-tenant, so these
// operations use the BYPASSRLS admin pool.
type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

var ErrExists = errors.New("resource already exists")

func CreateTenant(ctx context.Context, pool *pgxpool.Pool, name, slug string) (Tenant, error) {
	var t Tenant
	err := pool.QueryRow(ctx,
		`INSERT INTO tenants (name, slug) VALUES ($1, $2)
		 RETURNING id, name, slug, status, created_at`,
		name, slug,
	).Scan(&t.ID, &t.Name, &t.Slug, &t.Status, &t.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Tenant{}, ErrExists
		}
		return Tenant{}, err
	}
	return t, nil
}

func ListTenants(ctx context.Context, pool *pgxpool.Pool) ([]Tenant, error) {
	rows, err := pool.Query(ctx,
		`SELECT id, name, slug, status, created_at FROM tenants ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.ID, &t.Name, &t.Slug, &t.Status, &t.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func GetTenantBySlug(ctx context.Context, pool *pgxpool.Pool, slug string) (Tenant, error) {
	var t Tenant
	err := pool.QueryRow(ctx,
		`SELECT id, name, slug, status, created_at FROM tenants WHERE slug=$1`, slug,
	).Scan(&t.ID, &t.Name, &t.Slug, &t.Status, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	return t, err
}

// NewTenantID returns a fresh uuid string for tenant-scoped fixtures.
func NewTenantID() string { return uuid.NewString() }
