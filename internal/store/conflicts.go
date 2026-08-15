package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// ConflictRecord is one unsynchronized (or operator-resolved) conflict between
// two source states of the same canonical entity.
type ConflictRecord struct {
	ID                 string     `json:"id"`
	TenantID           string     `json:"tenant_id"`
	EntityType         string     `json:"entity_type"`
	EntityID           string     `json:"entity_id"`
	SourceA            string     `json:"source_a"`
	VersionA           int64      `json:"version_a"`
	PayloadA           []byte     `json:"payload_a"`
	SourceB            string     `json:"source_b"`
	VersionB           int64      `json:"version_b"`
	PayloadB           []byte     `json:"payload_b"`
	DetectedAt         time.Time  `json:"detected_at"`
	Status             string     `json:"status"`
	ResolutionStrategy string     `json:"resolution_strategy"`
	ResolvedBy         *string    `json:"resolved_by"`
	ResolvedAt         *time.Time `json:"resolved_at"`
}

// Conflict statuses.
const (
	ConflictPending      = "CONFLICT_PENDING"
	ConflictResolved     = "RESOLVED"
	ConflictAutoResolved = "AUTO_RESOLVED"
	ConflictDismissed    = "DISMISSED"
)

// InsertConflict records a detected conflict. Idempotent per the unique
// (tenant, entity, source pair, versions) key: a concurrent redelivery of the
// same logical pair is a no-op (returns the existing row). When an entry was
// previously resolved/dismissed and the exact same pair comes again, it is
// left alone — the operator's decision stands.
func InsertConflict(ctx context.Context, pool *pgxpool.Pool, c ConflictRecord) (ConflictRecord, error) {
	var out ConflictRecord
	_, err := db.WithTenant[struct{}](ctx, pool, c.TenantID, func(tx pgx.Tx) (struct{}, error) {
		err := tx.QueryRow(ctx,
			`INSERT INTO conflicts
			   (tenant_id, entity_type, entity_id, source_a, version_a, payload_a,
			    source_b, version_b, payload_b, status, resolution_strategy)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			 ON CONFLICT (tenant_id, entity_type, entity_id, source_a, source_b, version_a, version_b) DO NOTHING
			 RETURNING id, tenant_id, entity_type, entity_id, source_a, version_a, payload_a,
			           source_b, version_b, payload_b, detected_at, status, resolution_strategy,
			           resolved_by, resolved_at`,
			c.TenantID, c.EntityType, c.EntityID, c.SourceA, c.VersionA, c.PayloadA,
			c.SourceB, c.VersionB, c.PayloadB, c.Status, c.ResolutionStrategy,
		).Scan(&out.ID, &out.TenantID, &out.EntityType, &out.EntityID, &out.SourceA, &out.VersionA,
			&out.PayloadA, &out.SourceB, &out.VersionB, &out.PayloadB, &out.DetectedAt, &out.Status,
			&out.ResolutionStrategy, &out.ResolvedBy, &out.ResolvedAt)
		if err == nil {
			return struct{}{}, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return struct{}{}, err
		}
		// Duplicate detection: load the existing row for the same key.
		derr := tx.QueryRow(ctx,
			`SELECT id, tenant_id, entity_type, entity_id, source_a, version_a, payload_a,
			        source_b, version_b, payload_b, detected_at, status, resolution_strategy,
			        resolved_by, resolved_at
			 FROM conflicts
			 WHERE tenant_id=$1 AND entity_type=$2 AND entity_id=$3 AND source_a=$4
			   AND source_b=$5 AND version_a=$6 AND version_b=$7`,
			c.TenantID, c.EntityType, c.EntityID, c.SourceA, c.SourceB, c.VersionA, c.VersionB,
		).Scan(&out.ID, &out.TenantID, &out.EntityType, &out.EntityID, &out.SourceA, &out.VersionA,
			&out.PayloadA, &out.SourceB, &out.VersionB, &out.PayloadB, &out.DetectedAt, &out.Status,
			&out.ResolutionStrategy, &out.ResolvedBy, &out.ResolvedAt)
		return struct{}{}, derr
	})
	if err != nil {
		return ConflictRecord{}, err
	}
	return out, nil
}

// ListConflicts returns conflicts for a tenant, optional status filter.
func ListConflicts(ctx context.Context, pool *pgxpool.Pool, tenantID, status string, limit int) ([]ConflictRecord, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	out, err := db.WithTenant[[]ConflictRecord](ctx, pool, tenantID, func(tx pgx.Tx) ([]ConflictRecord, error) {
		query := `SELECT id, tenant_id, entity_type, entity_id, source_a, version_a, payload_a,
		                 source_b, version_b, payload_b, detected_at, status, resolution_strategy,
		                 resolved_by, resolved_at
		          FROM conflicts`
		args := []any{limit}
		if status != "" {
			query += ` WHERE status=$2`
			args = append(args, status)
		}
		query += ` ORDER BY detected_at DESC LIMIT $1`
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []ConflictRecord
		for rows.Next() {
			var c ConflictRecord
			if err := rows.Scan(&c.ID, &c.TenantID, &c.EntityType, &c.EntityID, &c.SourceA, &c.VersionA,
				&c.PayloadA, &c.SourceB, &c.VersionB, &c.PayloadB, &c.DetectedAt, &c.Status,
				&c.ResolutionStrategy, &c.ResolvedBy, &c.ResolvedAt); err != nil {
				return nil, err
			}
			res = append(res, c)
		}
		return res, rows.Err()
	})
	return out, err
}

// GetConflict fetches a single conflict within a tenant.
func GetConflict(ctx context.Context, pool *pgxpool.Pool, tenantID, id string) (ConflictRecord, error) {
	out, err := db.WithTenant[ConflictRecord](ctx, pool, tenantID, func(tx pgx.Tx) (ConflictRecord, error) {
		var c ConflictRecord
		err := tx.QueryRow(ctx,
			`SELECT id, tenant_id, entity_type, entity_id, source_a, version_a, payload_a,
			        source_b, version_b, payload_b, detected_at, status, resolution_strategy,
			        resolved_by, resolved_at
			 FROM conflicts WHERE id=$1`, id,
		).Scan(&c.ID, &c.TenantID, &c.EntityType, &c.EntityID, &c.SourceA, &c.VersionA,
			&c.PayloadA, &c.SourceB, &c.VersionB, &c.PayloadB, &c.DetectedAt, &c.Status,
			&c.ResolutionStrategy, &c.ResolvedBy, &c.ResolvedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ConflictRecord{}, ErrNotFound
		}
		return c, err
	})
	return out, err
}

// SetConflictStatus transitions a conflict's status (pending -> resolved /
// dismissed / auto_resolved).
func SetConflictStatus(ctx context.Context, pool *pgxpool.Pool, tenantID, id, status, strategy, by string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE conflicts SET status=$1, resolution_strategy=$2, resolved_by=$3, resolved_at=now()
			 WHERE id=$4 AND status <> $5`,
			status, strategy, by, id, ConflictDismissed)
		return err
	})
}
