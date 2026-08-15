package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// DeadLetter is an event that failed permanently (or exhausted its retries)
// and awaits operator action. payload holds the serialized canonical event.
type DeadLetter struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	EventID    string     `json:"event_id"`
	Reason     string     `json:"reason"`
	ErrorClass string     `json:"error_class"`
	Payload    []byte     `json:"payload"`
	Status     string     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	ResolvedAt *time.Time `json:"resolved_at"`
}

// InsertDeadLetter durably records a dead-letter event. The unique
// (tenant_id, event_id) key makes DLQ writes idempotent: re-failing an event
// that is already dead (e.g. after a replayed retry exhausted again) reopens
// the same entry instead of creating a duplicate. When an entry was previously
// resolved/discarded and the same event fails again it is reopened.
func InsertDeadLetter(ctx context.Context, pool *pgxpool.Pool, d DeadLetter) (DeadLetter, error) {
	var out DeadLetter
	_, err := db.WithTenant[struct{}](ctx, pool, d.TenantID, func(tx pgx.Tx) (struct{}, error) {
		return struct{}{}, tx.QueryRow(ctx,
			`INSERT INTO dead_letter (tenant_id, event_id, reason, error_class, payload, status)
			 VALUES ($1,$2,$3,$4,$5,'open')
			 ON CONFLICT (tenant_id, event_id) DO UPDATE SET
			   reason = EXCLUDED.reason,
			   error_class = EXCLUDED.error_class,
			   payload = EXCLUDED.payload,
			   status = 'open',
			   resolved_at = NULL
			 RETURNING id, tenant_id, event_id, reason, error_class, payload, status,
			           created_at, resolved_at`,
			d.TenantID, d.EventID, d.Reason, d.ErrorClass, d.Payload,
		).Scan(&out.ID, &out.TenantID, &out.EventID, &out.Reason, &out.ErrorClass,
			&out.Payload, &out.Status, &out.CreatedAt, &out.ResolvedAt)
	})
	if err != nil {
		return DeadLetter{}, err
	}
	return out, nil
}

// ListDeadLetters returns dead-letter entries for a tenant, newest first.
func ListDeadLetters(ctx context.Context, pool *pgxpool.Pool, tenantID, status string, limit int) ([]DeadLetter, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	out, err := db.WithTenant[[]DeadLetter](ctx, pool, tenantID, func(tx pgx.Tx) ([]DeadLetter, error) {
		query := `SELECT id, tenant_id, event_id, reason, error_class, payload, status,
		                 created_at, resolved_at
		          FROM dead_letter`
		args := []any{limit}
		if status != "" {
			query += ` WHERE status=$2`
			args = append(args, status)
		}
		query += ` ORDER BY created_at DESC LIMIT $1`
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []DeadLetter
		for rows.Next() {
			var d DeadLetter
			if err := rows.Scan(&d.ID, &d.TenantID, &d.EventID, &d.Reason, &d.ErrorClass,
				&d.Payload, &d.Status, &d.CreatedAt, &d.ResolvedAt); err != nil {
				return nil, err
			}
			res = append(res, d)
		}
		return res, rows.Err()
	})
	return out, err
}

// GetDeadLetter fetches one dead-letter entry within a tenant.
func GetDeadLetter(ctx context.Context, pool *pgxpool.Pool, tenantID, id string) (DeadLetter, error) {
	out, err := db.WithTenant[DeadLetter](ctx, pool, tenantID, func(tx pgx.Tx) (DeadLetter, error) {
		var d DeadLetter
		err := tx.QueryRow(ctx,
			`SELECT id, tenant_id, event_id, reason, error_class, payload, status,
			        created_at, resolved_at
			 FROM dead_letter WHERE id=$1`, id,
		).Scan(&d.ID, &d.TenantID, &d.EventID, &d.Reason, &d.ErrorClass,
			&d.Payload, &d.Status, &d.CreatedAt, &d.ResolvedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return DeadLetter{}, ErrNotFound
		}
		return d, err
	})
	return out, err
}

// SetDeadLetterStatus transitions a dead-letter entry's status (open ->
// retrying/discarded/resolved).
func SetDeadLetterStatus(ctx context.Context, pool *pgxpool.Pool, tenantID, id, status string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE dead_letter SET status=$1, resolved_at=now() WHERE id=$2`,
			status, id)
		return err
	})
}

// ResolveDeadLetterForEvent marks a dead-letter entry resolved (used when a
// replayed retry eventually succeeds). Matches on the logical event key.
func ResolveDeadLetterForEvent(ctx context.Context, pool *pgxpool.Pool, tenantID, eventID string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE dead_letter SET status='resolved', resolved_at=now()
			 WHERE event_id=$1 AND status <> 'discarded'`,
			eventID)
		return err
	})
}

// SetSourceEventStatusTo marks an event's status without a FROM guard. Missing
// rows are a benign no-op. Used to flag events that reached the DLQ.
func SetSourceEventStatusTo(ctx context.Context, pool *pgxpool.Pool, tenantID, eventID, to string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE source_events SET status=$1 WHERE event_id=$2`, to, eventID)
		return err
	})
}
