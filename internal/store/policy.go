package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// SyncPolicy defines how a tenant synchronizes one entity between a source and
// a destination.
type SyncPolicy struct {
	ID               string    `json:"id"`
	TenantID         string    `json:"tenant_id"`
	Entity           string    `json:"entity"`
	Source           string    `json:"source"`
	Destination      string    `json:"destination"`
	Mode             string    `json:"mode"`
	ConflictStrategy string    `json:"conflict_strategy"`
	DeletePolicy     string    `json:"delete_policy"`
	RetryPolicy      string    `json:"retry_policy"`
	SourcePriority   int       `json:"source_priority"`
	Enabled          bool      `json:"enabled"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func GetSyncPolicy(ctx context.Context, pool *pgxpool.Pool, tenant, entity, source, destination string) (SyncPolicy, error) {
	out, err := db.WithTenant[SyncPolicy](ctx, pool, tenant, func(tx pgx.Tx) (SyncPolicy, error) {
		var p SyncPolicy
		err := tx.QueryRow(ctx,
			`SELECT id, tenant_id, entity, source, destination, mode, conflict_strategy,
			        delete_policy, retry_policy, source_priority, enabled, created_at, updated_at
			 FROM sync_policies
			 WHERE entity=$1 AND source=$2 AND destination=$3 AND enabled=true`,
			entity, source, destination,
		).Scan(&p.ID, &p.TenantID, &p.Entity, &p.Source, &p.Destination, &p.Mode, &p.ConflictStrategy,
			&p.DeletePolicy, &p.RetryPolicy, &p.SourcePriority, &p.Enabled, &p.CreatedAt, &p.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return SyncPolicy{}, ErrNotFound
		}
		return p, err
	})
	return out, err
}

// UpsertSyncPolicy creates or updates a policy (idempotent seeding).
func UpsertSyncPolicy(ctx context.Context, pool *pgxpool.Pool, p SyncPolicy) (SyncPolicy, error) {
	var out SyncPolicy
	_, err := db.WithTenant[struct{}](ctx, pool, p.TenantID, func(tx pgx.Tx) (struct{}, error) {
		return struct{}{}, tx.QueryRow(ctx,
			`INSERT INTO sync_policies
			   (tenant_id, entity, source, destination, mode, conflict_strategy, delete_policy, retry_policy, source_priority, enabled)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			 ON CONFLICT (tenant_id, entity, source, destination) DO UPDATE SET
			   mode=EXCLUDED.mode, conflict_strategy=EXCLUDED.conflict_strategy,
			   delete_policy=EXCLUDED.delete_policy, retry_policy=EXCLUDED.retry_policy,
			   source_priority=EXCLUDED.source_priority, enabled=EXCLUDED.enabled, updated_at=now()
			 RETURNING id, tenant_id, entity, source, destination, mode, conflict_strategy,
			           delete_policy, retry_policy, source_priority, enabled, created_at, updated_at`,
			p.TenantID, p.Entity, p.Source, p.Destination, p.Mode, p.ConflictStrategy, p.DeletePolicy,
			p.RetryPolicy, p.SourcePriority, p.Enabled,
		).Scan(&out.ID, &out.TenantID, &out.Entity, &out.Source, &out.Destination, &out.Mode,
			&out.ConflictStrategy, &out.DeletePolicy, &out.RetryPolicy, &out.SourcePriority, &out.Enabled,
			&out.CreatedAt, &out.UpdatedAt)
	})
	if err != nil {
		return SyncPolicy{}, err
	}
	return out, nil
}

// ListPolicies returns all enabled policies for a tenant.
func ListPolicies(ctx context.Context, pool *pgxpool.Pool, tenantID string) ([]SyncPolicy, error) {
	out, err := db.WithTenant[[]SyncPolicy](ctx, pool, tenantID, func(tx pgx.Tx) ([]SyncPolicy, error) {
		rows, err := tx.Query(ctx,
			`SELECT id, tenant_id, entity, source, destination, mode, conflict_strategy,
			        delete_policy, retry_policy, source_priority, enabled, created_at, updated_at
			 FROM sync_policies WHERE enabled=true ORDER BY created_at`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []SyncPolicy
		for rows.Next() {
			var p SyncPolicy
			if err := rows.Scan(&p.ID, &p.TenantID, &p.Entity, &p.Source, &p.Destination, &p.Mode,
				&p.ConflictStrategy, &p.DeletePolicy, &p.RetryPolicy, &p.SourcePriority, &p.Enabled,
				&p.CreatedAt, &p.UpdatedAt); err != nil {
				return nil, err
			}
			res = append(res, p)
		}
		return res, rows.Err()
	})
	return out, err
}
