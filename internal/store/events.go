package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// SourceEvent is a raw, provider-native webhook event that SyncForge has
// ingested. Events are immutable once stored.
type SourceEvent struct {
	ID            string
	TenantID      string
	Source        string
	EventID       string
	EntityType    string
	EntityID      string
	EventType     string
	SourceVersion int64
	OccurredAt    *time.Time
	ReceivedAt    time.Time
	CorrelationID string
	Provenance    map[string]any
	Raw           map[string]any
	Status        string
}

// ErrDuplicate is returned when the same logical event (tenant, source,
// event_id) has already been ingested.
var ErrDuplicate = errors.New("duplicate event")

// InsertSourceEvent durably records an ingested webhook. The unique
// (tenant_id, source, event_id) constraint guarantees exactly-once ingestion:
// resubmissions are rejected as duplicates.
func InsertSourceEvent(ctx context.Context, pool *pgxpool.Pool, ev SourceEvent) (SourceEvent, error) {
	var out SourceEvent
	_, err := db.WithTenant[struct{}](ctx, pool, ev.TenantID, func(tx pgx.Tx) (struct{}, error) {
		err := tx.QueryRow(ctx,
			`INSERT INTO source_events
			   (tenant_id, source, event_id, entity_type, entity_id, event_type, source_version, occurred_at, correlation_id, provenance, raw, status)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'received')
			 ON CONFLICT (tenant_id, source, event_id) DO NOTHING
			 RETURNING id, tenant_id, source, event_id, entity_type, entity_id, event_type,
			           source_version, occurred_at, received_at, correlation_id, provenance, raw, status`,
			ev.TenantID, ev.Source, ev.EventID, ev.EntityType, ev.EntityID, ev.EventType,
			ev.SourceVersion, ev.OccurredAt, ev.CorrelationID, ev.Provenance, ev.Raw,
		).Scan(&out.ID, &out.TenantID, &out.Source, &out.EventID, &out.EntityType, &out.EntityID,
			&out.EventType, &out.SourceVersion, &out.OccurredAt, &out.ReceivedAt, &out.CorrelationID,
			&out.Provenance, &out.Raw, &out.Status)
		if err == pgx.ErrNoRows {
			return struct{}{}, ErrDuplicate
		}
		return struct{}{}, err
	})
	if err != nil {
		return SourceEvent{}, err
	}
	return out, nil
}

// GetSourceEvent fetches a stored source event within a tenant.
func GetSourceEvent(ctx context.Context, pool *pgxpool.Pool, tenantID, eventID string) (SourceEvent, error) {
	out, err := db.WithTenant[SourceEvent](ctx, pool, tenantID, func(tx pgx.Tx) (SourceEvent, error) {
		var ev SourceEvent
		err := tx.QueryRow(ctx,
			`SELECT id, tenant_id, source, event_id, entity_type, entity_id, event_type,
			        source_version, occurred_at, received_at, correlation_id, provenance, raw, status
			 FROM source_events WHERE event_id=$1`, eventID,
		).Scan(&ev.ID, &ev.TenantID, &ev.Source, &ev.EventID, &ev.EntityType, &ev.EntityID,
			&ev.EventType, &ev.SourceVersion, &ev.OccurredAt, &ev.ReceivedAt, &ev.CorrelationID,
			&ev.Provenance, &ev.Raw, &ev.Status)
		if errors.Is(err, pgx.ErrNoRows) {
			return SourceEvent{}, ErrNotFound
		}
		return ev, err
	})
	return out, err
}

// ListSourceEvents returns recent ingested events for a tenant.
func ListSourceEvents(ctx context.Context, pool *pgxpool.Pool, tenantID string, limit int) ([]SourceEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	out, err := db.WithTenant[[]SourceEvent](ctx, pool, tenantID, func(tx pgx.Tx) ([]SourceEvent, error) {
		rows, err := tx.Query(ctx,
			`SELECT id, tenant_id, source, event_id, entity_type, entity_id, event_type,
			        source_version, occurred_at, received_at, correlation_id, provenance, raw, status
			 FROM source_events ORDER BY received_at DESC LIMIT $1`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []SourceEvent
		for rows.Next() {
			var ev SourceEvent
			if err := rows.Scan(&ev.ID, &ev.TenantID, &ev.Source, &ev.EventID, &ev.EntityType, &ev.EntityID,
				&ev.EventType, &ev.SourceVersion, &ev.OccurredAt, &ev.ReceivedAt, &ev.CorrelationID,
				&ev.Provenance, &ev.Raw, &ev.Status); err != nil {
				return nil, err
			}
			res = append(res, ev)
		}
		return res, rows.Err()
	})
	return out, err
}

// SetSourceEventStatus transitions an event from one status to another
// (e.g. received -> validated, validated -> processed/failed). The FROM guard
// makes status transitions atomic; a missing or already-transitioned row is a
// benign no-op.
func SetSourceEventStatus(ctx context.Context, pool *pgxpool.Pool, tenant, eventID, from, to string) error {
	return db.WithTenantTx(ctx, pool, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE source_events SET status=$1 WHERE event_id=$2 AND status=$3`,
			to, eventID, from)
		return err
	})
}

// PendingSourceEvents scans for not-yet-published events across all tenants.
// Runs on the admin pool (cross-tenant) with row locking so ingestion workers
// can run concurrently. Returns events with status='received'.
func PendingSourceEvents(ctx context.Context, pool *pgxpool.Pool, limit int) ([]SourceEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := pool.Query(ctx,
		`SELECT id, tenant_id, source, event_id, entity_type, entity_id, event_type,
		        source_version, occurred_at, correlation_id, provenance, raw
		 FROM source_events
		 WHERE status='received'
		 ORDER BY received_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []SourceEvent
	for rows.Next() {
		var ev SourceEvent
		if err := rows.Scan(&ev.ID, &ev.TenantID, &ev.Source, &ev.EventID, &ev.EntityType, &ev.EntityID,
			&ev.EventType, &ev.SourceVersion, &ev.OccurredAt, &ev.CorrelationID, &ev.Provenance, &ev.Raw); err != nil {
			return nil, err
		}
		ev.Status = "received"
		res = append(res, ev)
	}
	return res, rows.Err()
}
