package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// OutboundWrite records the fingerprint of what SyncForge last wrote to a
// destination for a canonical entity. It is the loop-prevention ledger: an
// incoming event that normalizes to the same fingerprint we sent is our own
// echo and must not be propagated again.
type OutboundWrite struct {
	TenantID       string
	EntityType     string
	EntityID       string
	TargetSource   string
	Fingerprint    string
	AppliedVersion int64
	WrittenAt      time.Time
}

func UpsertOutboundWrite(ctx context.Context, pool *pgxpool.Pool, w OutboundWrite) error {
	return db.WithTenantTx(ctx, pool, w.TenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO outbound_writes (tenant_id, entity_type, entity_id, target_source, fingerprint, applied_version)
			 VALUES ($1,$2,$3,$4,$5,$6)
			 ON CONFLICT (tenant_id, entity_type, entity_id, target_source) DO UPDATE SET
			   fingerprint=EXCLUDED.fingerprint, applied_version=EXCLUDED.applied_version, written_at=now()`,
			w.TenantID, w.EntityType, w.EntityID, w.TargetSource, w.Fingerprint, w.AppliedVersion)
		return err
	})
}

// GetOutboundWrite returns the fingerprint last written to targetSource for an
// entity. ErrNotFound when none recorded yet.
func GetOutboundWrite(ctx context.Context, pool *pgxpool.Pool, tenant, entityType, entityID, targetSource string) (OutboundWrite, error) {
	out, err := db.WithTenant[OutboundWrite](ctx, pool, tenant, func(tx pgx.Tx) (OutboundWrite, error) {
		var w OutboundWrite
		err := tx.QueryRow(ctx,
			`SELECT tenant_id, entity_type, entity_id, target_source, fingerprint, applied_version, written_at
			 FROM outbound_writes
			 WHERE tenant_id=$1 AND entity_type=$2 AND entity_id=$3 AND target_source=$4`,
			tenant, entityType, entityID, targetSource,
		).Scan(&w.TenantID, &w.EntityType, &w.EntityID, &w.TargetSource, &w.Fingerprint, &w.AppliedVersion, &w.WrittenAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return OutboundWrite{}, ErrNotFound
		}
		return w, err
	})
	return out, err
}
