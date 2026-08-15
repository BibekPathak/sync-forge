package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// RetryEntry is a durable, tenant-scoped retry. The retry engine drains rows
// whose next_attempt_at has passed. state holds the serialized canonical event
// so a retry can be re-processed without touching the broker.
type RetryEntry struct {
	ID            string
	TenantID      string
	EventID       string
	Attempt       int
	MaxAttempts   int
	NextAttemptAt time.Time
	LastError     string
	ErrorClass    string
	State         []byte
	UpdatedAt     time.Time
}

// EnqueueRetry creates (or advances) a retry row for one logical event.
// next_attempt_at is set to now()+delay so the retry engine picks it up after
// backoff. The unique (tenant_id, event_id) key makes enqueue idempotent: a
// duplicate or concurrent redelivery merely increments the attempt counter.
func EnqueueRetry(ctx context.Context, pool *pgxpool.Pool, e RetryEntry, delay time.Duration) (RetryEntry, error) {
	var out RetryEntry
	_, err := db.WithTenant[struct{}](ctx, pool, e.TenantID, func(tx pgx.Tx) (struct{}, error) {
		return struct{}{}, tx.QueryRow(ctx,
			`INSERT INTO retry_queue (tenant_id, event_id, attempt, max_attempts, next_attempt_at, last_error, error_class, state)
			 VALUES ($1,$2,1,$3, now() + make_interval(secs => $4), $5, $6, $7)
			 ON CONFLICT (tenant_id, event_id) DO UPDATE SET
			   attempt = retry_queue.attempt + 1,
			   next_attempt_at = now() + make_interval(secs => $4),
			   last_error = EXCLUDED.last_error,
			   error_class = EXCLUDED.error_class,
			   state = EXCLUDED.state,
			   updated_at = now()
			 RETURNING id, tenant_id, event_id, attempt, max_attempts, next_attempt_at,
			           last_error, error_class, state::text, updated_at`,
			e.TenantID, e.EventID, e.MaxAttempts, delay.Seconds(), e.LastError, e.ErrorClass, e.State,
		).Scan(&out.ID, &out.TenantID, &out.EventID, &out.Attempt, &out.MaxAttempts,
			&out.NextAttemptAt, &out.LastError, &out.ErrorClass, &out.State, &out.UpdatedAt)
	})
	if err != nil {
		return RetryEntry{}, err
	}
	return out, nil
}

// ClaimDueRetries returns retry rows that are due, locking them so concurrent
// retry workers do not double-process. Runs on the admin pool (cross-tenant):
// each entry carries its tenant_id and events are processed inside a tenant
// context.
func ClaimDueRetries(ctx context.Context, pool *pgxpool.Pool, limit int) ([]RetryEntry, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := pool.Query(ctx,
		`SELECT id, tenant_id, event_id, attempt, max_attempts, next_attempt_at,
		        last_error, error_class, state::text
		 FROM retry_queue
		 WHERE next_attempt_at <= now()
		 ORDER BY next_attempt_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []RetryEntry
	for rows.Next() {
		var e RetryEntry
		if err := rows.Scan(&e.ID, &e.TenantID, &e.EventID, &e.Attempt, &e.MaxAttempts,
			&e.NextAttemptAt, &e.LastError, &e.ErrorClass, &e.State); err != nil {
			return nil, err
		}
		res = append(res, e)
	}
	return res, rows.Err()
}

// DeleteRetry removes a retry row once the event has been processed
// successfully.
func DeleteRetry(ctx context.Context, pool *pgxpool.Pool, tenantID, eventID string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`DELETE FROM retry_queue WHERE tenant_id=$1 AND event_id=$2`,
			tenantID, eventID)
		return err
	})
}
