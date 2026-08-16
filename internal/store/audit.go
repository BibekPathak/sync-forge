package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// AuditLog is one immutable operator/security event: who did what to which
// resource. tenant_id is nullable so login failures (which occur before a
// tenant context exists) can be recorded via the admin pool.
type AuditLog struct {
	ID         string         `json:"id"`
	TenantID   *string        `json:"tenant_id,omitempty"`
	Actor      string         `json:"actor"`
	Action     string         `json:"action"`
	Resource   string         `json:"resource"`
	ResourceID string         `json:"resource_id,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
}

// InsertAuditLog appends an immutable audit event inside the caller's tenant
// context so RLS enforces the WITH CHECK on tenant_id. Login failures pass the
// admin pool (BYPASSRLS) with a nil tenant.
func InsertAuditLog(ctx context.Context, pool *pgxpool.Pool, tenantID string, e AuditLog) (AuditLog, error) {
	e.Metadata = coalesceMap(e.Metadata)
	var out AuditLog
	var tenantPtr *string
	if tenantID != "" {
		tenantPtr = &tenantID
		e.TenantID = tenantPtr
	}
	_, err := db.WithTenant[struct{}](ctx, pool, tenantID, func(tx pgx.Tx) (struct{}, error) {
		err := tx.QueryRow(ctx,
			`INSERT INTO audit_log (tenant_id, actor, action, resource, resource_id, metadata)
			 VALUES ($1,$2,$3,$4,$5,$6)
			 RETURNING id, tenant_id, actor, action, resource, resource_id, metadata, created_at`,
			tenantPtr, e.Actor, e.Action, e.Resource, e.ResourceID, e.Metadata,
		).Scan(&out.ID, &out.TenantID, &out.Actor, &out.Action, &out.Resource,
			&out.ResourceID, &out.Metadata, &out.CreatedAt)
		return struct{}{}, err
	})
	return out, err
}

// ListAuditLogs returns the tenant's audit events, newest first, optionally
// filtered by actor/action/resource.
func ListAuditLogs(ctx context.Context, pool *pgxpool.Pool, tenantID, actor, action, resource string, limit int) ([]AuditLog, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	out, err := db.WithTenant[[]AuditLog](ctx, pool, tenantID, func(tx pgx.Tx) ([]AuditLog, error) {
		q := `SELECT id, tenant_id, actor, action, resource, resource_id, metadata, created_at
			  FROM audit_log WHERE true`
		args := []any{}
		if actor != "" {
			args = append(args, actor)
			q += ` AND actor=$` + itoa(len(args))
		}
		if action != "" {
			args = append(args, action)
			q += ` AND action=$` + itoa(len(args))
		}
		if resource != "" {
			args = append(args, resource)
			q += ` AND resource=$` + itoa(len(args))
		}
		args = append(args, limit)
		q += ` ORDER BY created_at DESC LIMIT $` + itoa(len(args))
		rows, err := tx.Query(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []AuditLog
		for rows.Next() {
			var a AuditLog
			if err := rows.Scan(&a.ID, &a.TenantID, &a.Actor, &a.Action, &a.Resource,
				&a.ResourceID, &a.Metadata, &a.CreatedAt); err != nil {
				return nil, err
			}
			res = append(res, a)
		}
		return res, rows.Err()
	})
	return out, err
}

// SyncOperation is one applied destination write: the ledger that records every
// write SyncForge made to an external system, backing loop-prevention forensics
// and the "every write is auditable" guarantee.
type SyncOperation struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	EntityType     string    `json:"entity_type"`
	EntityID       string    `json:"entity_id"`
	Source         string    `json:"source"`
	TargetSource   string    `json:"target_source"`
	EventID        string    `json:"event_id,omitempty"`
	AppliedVersion int64     `json:"applied_version"`
	Fingerprint    string    `json:"fingerprint"`
	CreatedAt      time.Time `json:"created_at"`
}

// InsertSyncOperation records an applied destination write.
func InsertSyncOperation(ctx context.Context, pool *pgxpool.Pool, op SyncOperation) (SyncOperation, error) {
	var out SyncOperation
	_, err := db.WithTenant[struct{}](ctx, pool, op.TenantID, func(tx pgx.Tx) (struct{}, error) {
		err := tx.QueryRow(ctx,
			`INSERT INTO sync_operations (tenant_id, entity_type, entity_id, source, target_source, event_id, applied_version, fingerprint)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			 RETURNING id, tenant_id, entity_type, entity_id, source, target_source, event_id, applied_version, fingerprint, created_at`,
			op.TenantID, op.EntityType, op.EntityID, op.Source, op.TargetSource,
			op.EventID, op.AppliedVersion, op.Fingerprint,
		).Scan(&out.ID, &out.TenantID, &out.EntityType, &out.EntityID, &out.Source,
			&out.TargetSource, &out.EventID, &out.AppliedVersion, &out.Fingerprint, &out.CreatedAt)
		return struct{}{}, err
	})
	return out, err
}

// ListSyncOperations lists the tenant's applied writes, newest first, for a
// given entity or target source.
func ListSyncOperations(ctx context.Context, pool *pgxpool.Pool, tenantID, entityType, entityID, targetSource string, limit int) ([]SyncOperation, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	out, err := db.WithTenant[[]SyncOperation](ctx, pool, tenantID, func(tx pgx.Tx) ([]SyncOperation, error) {
		q := `SELECT id, tenant_id, entity_type, entity_id, source, target_source, event_id, applied_version, fingerprint, created_at
			  FROM sync_operations WHERE true`
		args := []any{}
		if entityType != "" {
			args = append(args, entityType)
			q += ` AND entity_type=$` + itoa(len(args))
		}
		if entityID != "" {
			args = append(args, entityID)
			q += ` AND entity_id=$` + itoa(len(args))
		}
		if targetSource != "" {
			args = append(args, targetSource)
			q += ` AND target_source=$` + itoa(len(args))
		}
		args = append(args, limit)
		q += ` ORDER BY created_at DESC LIMIT $` + itoa(len(args))
		rows, err := tx.Query(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []SyncOperation
		for rows.Next() {
			var o SyncOperation
			if err := rows.Scan(&o.ID, &o.TenantID, &o.EntityType, &o.EntityID, &o.Source,
				&o.TargetSource, &o.EventID, &o.AppliedVersion, &o.Fingerprint, &o.CreatedAt); err != nil {
				return nil, err
			}
			res = append(res, o)
		}
		return res, rows.Err()
	})
	return out, err
}

func coalesceMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

// itoa is a tiny int->string helper to build positional placeholders.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
