package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// ClaimProcessedEvent durably marks a logical event as processed. The unique
// (tenant_id, source, event_id) primary key is the idempotency mechanism: a
// duplicate delivery cannot insert again. Returns claimed=false when the event
// was already processed (duplicate).
func ClaimProcessedEvent(ctx context.Context, pool *pgxpool.Pool, tenant, source, eventID, entityType, entityID string) (claimed bool, err error) {
	claimed, err = db.WithTenant[bool](ctx, pool, tenant, func(tx pgx.Tx) (bool, error) {
		tag, err := tx.Exec(ctx,
			`INSERT INTO processed_events (tenant_id, source, event_id, entity_type, entity_id)
			 VALUES ($1,$2,$3,$4,$5)
			 ON CONFLICT (tenant_id, source, event_id) DO NOTHING`,
			tenant, source, eventID, entityType, entityID)
		if err != nil {
			return false, err
		}
		return tag.RowsAffected() == 1, nil
	})
	return claimed, err
}

// ReleaseProcessedEvent removes an idempotency claim so a failed event can be
// retried. Called only on the failure path.
func ReleaseProcessedEvent(ctx context.Context, pool *pgxpool.Pool, tenant, source, eventID string) error {
	return db.WithTenantTx(ctx, pool, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`DELETE FROM processed_events WHERE tenant_id=$1 AND source=$2 AND event_id=$3`,
			tenant, source, eventID)
		return err
	})
}

// ProcessedCount returns the number of processed events for a tenant
// (used by tests to assert exactly-once processing).
func ProcessedCount(ctx context.Context, pool *pgxpool.Pool, tenant string) (int, error) {
	out, err := db.WithTenant[int](ctx, pool, tenant, func(tx pgx.Tx) (int, error) {
		var n int
		err := tx.QueryRow(ctx, `SELECT count(*) FROM processed_events WHERE tenant_id=$1`, tenant).Scan(&n)
		return n, err
	})
	return out, err
}

// ProcessedEventAt returns when an event was processed (for assertions).
func ProcessedEventAt(ctx context.Context, pool *pgxpool.Pool, tenant, source, eventID string) (time.Time, bool, error) {
	out, err := db.WithTenant[time.Time](ctx, pool, tenant, func(tx pgx.Tx) (time.Time, error) {
		var t time.Time
		err := tx.QueryRow(ctx,
			`SELECT processed_at FROM processed_events WHERE tenant_id=$1 AND source=$2 AND event_id=$3`,
			tenant, source, eventID).Scan(&t)
		return t, err
	})
	if err == ErrNotFound || err == pgx.ErrNoRows {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return out, true, nil
}
