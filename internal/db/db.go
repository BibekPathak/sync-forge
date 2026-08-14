package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB owns two connection pools:
//
//   - App: connects as the syncforge_app role. Row-Level Security is enforced
//     on every table, so all queries MUST run inside a tenant context
//     (see WithTenant).
//   - Admin: connects as the syncforge_engine role, which BYPASSRLS. Used for
//     cross-tenant administration (tenant management) and internal workers.
type DB struct {
	App   *pgxpool.Pool
	Admin *pgxpool.Pool
	Log   *slog.Logger
}

func Connect(ctx context.Context, appURL, adminURL string, log *slog.Logger) (*DB, error) {
	if log == nil {
		log = slog.Default()
	}

	app, err := newPool(ctx, appURL)
	if err != nil {
		return nil, fmt.Errorf("connect app pool: %w", err)
	}
	admin, err := newPool(ctx, adminURL)
	if err != nil {
		app.Close()
		return nil, fmt.Errorf("connect admin pool: %w", err)
	}

	return &DB{App: app, Admin: admin, Log: log}, nil
}

func newPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 20
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func (d *DB) Close() {
	if d.App != nil {
		d.App.Close()
	}
	if d.Admin != nil {
		d.Admin.Close()
	}
}

// WithTenant runs fn inside a transaction with app.tenant_id set, so
// Row-Level Security scopes all queries to the given tenant.
func WithTenant[T any](ctx context.Context, pool *pgxpool.Pool, tenantID string, fn func(tx pgx.Tx) (T, error)) (T, error) {
	var zero T
	tx, err := pool.Begin(ctx)
	if err != nil {
		return zero, err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenantID); err != nil {
		return zero, fmt.Errorf("set tenant context: %w", err)
	}

	res, err := fn(tx)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, err
	}
	return res, nil
}

// WithTenantTx is like WithTenant but the caller manages commits and may
// choose to run multiple operations and inspect results.
func WithTenantTx(ctx context.Context, pool *pgxpool.Pool, tenantID string, fn func(tx pgx.Tx) error) error {
	_, err := WithTenant[struct{}](ctx, pool, tenantID, func(tx pgx.Tx) (struct{}, error) {
		return struct{}{}, fn(tx)
	})
	return err
}
