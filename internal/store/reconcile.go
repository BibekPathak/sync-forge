package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// ReconciliationRun is a reconciliation sweep over one provider's records for
// a tenant. Mode selects whether findings are repaired automatically (auto) or
// parked for operator review (manual). Counters summarize the sweep results.
type ReconciliationRun struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	Entity     string     `json:"entity"`
	Source     string     `json:"source"`
	Mode       string     `json:"mode"`
	Status     string     `json:"status"`
	JobID      *string    `json:"job_id"`
	Total      int64      `json:"total"`
	Drift      int64      `json:"drift"`
	Missed     int64      `json:"missed"`
	Deleted    int64      `json:"deleted"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
}

// Reconcile run statuses.
const (
	ReconcileRunning  = "running"
	ReconcileComplete = "completed"
	ReconcileFailed   = "failed"
)

// Reconcile findings describe a single divergence between the canonical model
// and a provider: a field mismatch (drift), a provider record never ingested
// (missed), a record that must be deleted on the provider (deleted), or a
// canonical record missing from the provider (missing). Status tracks operator
// review for manual runs and repair state for auto runs.
type ReconciliationFinding struct {
	ID              string         `json:"id"`
	RunID           string         `json:"run_id"`
	TenantID        string         `json:"tenant_id"`
	Kind            string         `json:"kind"`
	ProviderID      string         `json:"provider_id"`
	CanonicalID     string         `json:"canonical_id,omitempty"`
	CanonicalFields map[string]any `json:"canonical_fields"`
	ProviderFields  map[string]any `json:"provider_fields"`
	ProviderVersion int64          `json:"provider_version"`
	Direction       string         `json:"direction"`
	Status          string         `json:"status"`
	Error           *string        `json:"error"`
	CreatedAt       time.Time      `json:"created_at"`
	AppliedAt       *time.Time     `json:"applied_at"`
}

// Reconcile finding kinds.
const (
	FindingMissed  = "missed"
	FindingDrift   = "drift"
	FindingDeleted = "deleted"
	FindingMissing = "missing"
)

// Reconcile finding statuses.
const (
	FindingPending   = "pending"
	FindingApplied   = "applied"
	FindingSkipped   = "skipped"
	FindingDismissed = "dismissed"
	FindingFailed    = "failed"
)

func scanRun(row pgx.Row) (ReconciliationRun, error) {
	var r ReconciliationRun
	err := row.Scan(&r.ID, &r.TenantID, &r.Entity, &r.Source, &r.Mode, &r.Status,
		&r.JobID, &r.Total, &r.Drift, &r.Missed, &r.Deleted, &r.StartedAt, &r.FinishedAt)
	return r, err
}

const runCols = `id, tenant_id, entity, source, mode, status, job_id, total, drift, missed, deleted, started_at, finished_at`

// CreateReconciliationRun records the start of a reconciliation sweep.
func CreateReconciliationRun(ctx context.Context, pool *pgxpool.Pool, r ReconciliationRun) (ReconciliationRun, error) {
	var out ReconciliationRun
	_, err := db.WithTenant[struct{}](ctx, pool, r.TenantID, func(tx pgx.Tx) (struct{}, error) {
		return struct{}{}, tx.QueryRow(ctx,
			`INSERT INTO reconciliation_runs (tenant_id, entity, source, mode, status, job_id)
			 VALUES ($1,$2,$3,$4,$5,$6) RETURNING `+runCols,
			r.TenantID, r.Entity, r.Source, r.Mode, r.Status, r.JobID,
		).Scan(&out.ID, &out.TenantID, &out.Entity, &out.Source, &out.Mode, &out.Status,
			&out.JobID, &out.Total, &out.Drift, &out.Missed, &out.Deleted, &out.StartedAt, &out.FinishedAt)
	})
	if err != nil {
		return ReconciliationRun{}, err
	}
	return out, nil
}

// GetReconciliationRun fetches a run within a tenant.
func GetReconciliationRun(ctx context.Context, pool *pgxpool.Pool, tenantID, id string) (ReconciliationRun, error) {
	out, err := db.WithTenant[ReconciliationRun](ctx, pool, tenantID, func(tx pgx.Tx) (ReconciliationRun, error) {
		r, err := scanRun(tx.QueryRow(ctx,
			`SELECT `+runCols+` FROM reconciliation_runs WHERE id=$1`, id))
		if errors.Is(err, pgx.ErrNoRows) {
			return ReconciliationRun{}, ErrNotFound
		}
		return r, err
	})
	return out, err
}

// GetReconciliationRunByJobID locates a run from its scheduling sync job.
// Runs on the admin pool: the reconcile runner is cross-tenant.
func GetReconciliationRunByJobID(ctx context.Context, pool *pgxpool.Pool, jobID string) (ReconciliationRun, error) {
	var r ReconciliationRun
	err := pool.QueryRow(ctx,
		`SELECT `+runCols+` FROM reconciliation_runs WHERE job_id=$1`, jobID,
	).Scan(&r.ID, &r.TenantID, &r.Entity, &r.Source, &r.Mode, &r.Status,
		&r.JobID, &r.Total, &r.Drift, &r.Missed, &r.Deleted, &r.StartedAt, &r.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReconciliationRun{}, ErrNotFound
	}
	return r, err
}

// ListReconciliationRuns returns a tenant's runs, newest first.
func ListReconciliationRuns(ctx context.Context, pool *pgxpool.Pool, tenantID string, limit int) ([]ReconciliationRun, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	out, err := db.WithTenant[[]ReconciliationRun](ctx, pool, tenantID, func(tx pgx.Tx) ([]ReconciliationRun, error) {
		rows, err := tx.Query(ctx,
			`SELECT `+runCols+` FROM reconciliation_runs ORDER BY created_at DESC LIMIT $1`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []ReconciliationRun
		for rows.Next() {
			r, err := scanRun(rows)
			if err != nil {
				return nil, err
			}
			res = append(res, r)
		}
		return res, rows.Err()
	})
	return out, err
}

// UpdateReconciliationCounters persists the sweep count gut. The admin pool is
// used so the reconcile runner can checkpoint across tenants.
func UpdateReconciliationCounters(ctx context.Context, pool *pgxpool.Pool, runID string, total, drift, missed, deleted int64) error {
	_, err := pool.Exec(ctx,
		`UPDATE reconciliation_runs SET total=$2, drift=$3, missed=$4, deleted=$5
		 WHERE id=$1`,
		runID, total, drift, missed, deleted)
	return err
}

// FinishReconciliationRun marks a run completed or failed.
func FinishReconciliationRun(ctx context.Context, pool *pgxpool.Pool, runID, status string, lastError *string) error {
	_, err := pool.Exec(ctx,
		`UPDATE reconciliation_runs SET status=$2, finished_at=now(), error=$3
		 WHERE id=$1`,
		runID, status, lastError)
	return err
}

func scanFinding(row pgx.Row) (ReconciliationFinding, error) {
	var f ReconciliationFinding
	err := row.Scan(&f.ID, &f.RunID, &f.TenantID, &f.Kind, &f.ProviderID, &f.CanonicalID,
		&f.CanonicalFields, &f.ProviderFields, &f.ProviderVersion, &f.Direction, &f.Status,
		&f.Error, &f.CreatedAt, &f.AppliedAt)
	return f, err
}

const findingCols = `id, run_id, tenant_id, kind, provider_id, canonical_id, canonical_fields, provider_fields, provider_version, direction, status, error, created_at, applied_at`

// InsertReconciliationFinding records a divergence. Idempotent per
// (run_id, kind, provider_id): a re-detection of the same divergence returns
// the existing row unchanged (an operator's decision stands).
func InsertReconciliationFinding(ctx context.Context, pool *pgxpool.Pool, f ReconciliationFinding) (ReconciliationFinding, error) {
	if f.Direction == "" {
		f.Direction = reconcileDirection(f.Kind)
	}
	if f.Status == "" {
		f.Status = FindingPending
	}
	// jsonb columns are NOT NULL: coalesce absent maps to empty objects.
	if f.CanonicalFields == nil {
		f.CanonicalFields = map[string]any{}
	}
	if f.ProviderFields == nil {
		f.ProviderFields = map[string]any{}
	}
	var out ReconciliationFinding
	_, err := db.WithTenant[struct{}](ctx, pool, f.TenantID, func(tx pgx.Tx) (struct{}, error) {
		err := tx.QueryRow(ctx,
			`INSERT INTO reconciliation_findings
			   (run_id, tenant_id, kind, provider_id, canonical_id, canonical_fields, provider_fields, provider_version, direction, status)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			 ON CONFLICT (run_id, kind, provider_id) DO NOTHING
			 RETURNING `+findingCols,
			f.RunID, f.TenantID, f.Kind, f.ProviderID, f.CanonicalID,
			f.CanonicalFields, f.ProviderFields, f.ProviderVersion, f.Direction, f.Status,
		).Scan(&out.ID, &out.RunID, &out.TenantID, &out.Kind, &out.ProviderID, &out.CanonicalID,
			&out.CanonicalFields, &out.ProviderFields, &out.ProviderVersion, &out.Direction, &out.Status,
			&out.Error, &out.CreatedAt, &out.AppliedAt)
		if err == nil {
			return struct{}{}, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return struct{}{}, err
		}
		derr := tx.QueryRow(ctx,
			`SELECT `+findingCols+` FROM reconciliation_findings
			 WHERE run_id=$1 AND kind=$2 AND provider_id=$3`,
			f.RunID, f.Kind, f.ProviderID,
		).Scan(&out.ID, &out.RunID, &out.TenantID, &out.Kind, &out.ProviderID, &out.CanonicalID,
			&out.CanonicalFields, &out.ProviderFields, &out.ProviderVersion, &out.Direction, &out.Status,
			&out.Error, &out.CreatedAt, &out.AppliedAt)
		return struct{}{}, derr
	})
	if err != nil {
		return ReconciliationFinding{}, err
	}
	return out, nil
}

// ListReconciliationFindings returns a run's findings, optionally filtered by
// status.
func ListReconciliationFindings(ctx context.Context, pool *pgxpool.Pool, tenantID, runID, status string, limit int) ([]ReconciliationFinding, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	out, err := db.WithTenant[[]ReconciliationFinding](ctx, pool, tenantID, func(tx pgx.Tx) ([]ReconciliationFinding, error) {
		query := `SELECT ` + findingCols + ` FROM reconciliation_findings WHERE run_id=$1`
		args := []any{runID}
		if status != "" {
			query += ` AND status=$2 ORDER BY created_at LIMIT $3`
			args = append(args, status, limit)
		} else {
			query += ` ORDER BY created_at LIMIT $2`
			args = append(args, limit)
		}
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []ReconciliationFinding
		for rows.Next() {
			f, err := scanFinding(rows)
			if err != nil {
				return nil, err
			}
			res = append(res, f)
		}
		return res, rows.Err()
	})
	return out, err
}

// GetReconciliationFinding fetches one finding within a tenant.
func GetReconciliationFinding(ctx context.Context, pool *pgxpool.Pool, tenantID, id string) (ReconciliationFinding, error) {
	out, err := db.WithTenant[ReconciliationFinding](ctx, pool, tenantID, func(tx pgx.Tx) (ReconciliationFinding, error) {
		f, err := scanFinding(tx.QueryRow(ctx,
			`SELECT `+findingCols+` FROM reconciliation_findings WHERE id=$1`, id))
		if errors.Is(err, pgx.ErrNoRows) {
			return ReconciliationFinding{}, ErrNotFound
		}
		return f, err
	})
	return out, err
}

// SetReconciliationFindingDirection overrides the repair direction of a pending
// finding (used when an operator picks a different side than the default).
func SetReconciliationFindingDirection(ctx context.Context, pool *pgxpool.Pool, tenantID, id, direction string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE reconciliation_findings SET direction=$1 WHERE id=$2`, direction, id)
		return err
	})
}

// CountReconciliationFindings aggregates a run's findings by kind, folding
// `missing` (canonical absent from provider) into `missed` (presence gaps) for
// the run's summary counters.
func CountReconciliationFindings(ctx context.Context, pool *pgxpool.Pool, tenantID, runID string) (drift, missed, deleted int64, err error) {
	_, err = db.WithTenant[struct{}](ctx, pool, tenantID, func(tx pgx.Tx) (struct{}, error) {
		rows, err := tx.Query(ctx,
			`SELECT kind, count(*) FROM reconciliation_findings WHERE run_id=$1 GROUP BY kind`, runID)
		if err != nil {
			return struct{}{}, err
		}
		defer rows.Close()
		for rows.Next() {
			var kind string
			var n int64
			if err := rows.Scan(&kind, &n); err != nil {
				return struct{}{}, err
			}
			switch kind {
			case FindingDrift:
				drift += n
			case FindingDeleted:
				deleted += n
			case FindingMissed, FindingMissing:
				missed += n
			}
		}
		return struct{}{}, rows.Err()
	})
	return drift, missed, deleted, err
}

// SetReconciliationFindingStatus transitions a finding's state. Applied and
// dismissed are terminal: a re-run of the same repair must not clear them.
func SetReconciliationFindingStatus(ctx context.Context, pool *pgxpool.Pool, tenantID, id, status string, updateErr *string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE reconciliation_findings
			 SET status=$1, error=$2, applied_at=COALESCE(applied_at, now())
			 WHERE id=$3
			   AND (status='pending'
			        OR (status='failed' AND $1='pending'))`,
			status, updateErr, id)
		return err
	})
}

// ListCanonicalByProvider returns canonical records that carry a provider id
// for the given provider. Used to detect records the provider no longer has
// (kind=missing).
func ListCanonicalByProvider(ctx context.Context, pool *pgxpool.Pool, tenantID, entityType, provider string) ([]CanonicalRecord, error) {
	out, err := db.WithTenant[[]CanonicalRecord](ctx, pool, tenantID, func(tx pgx.Tx) ([]CanonicalRecord, error) {
		rows, err := tx.Query(ctx,
			`SELECT sync_id, tenant_id, entity_type, entity_id, fields, version, source_versions,
			        field_provenance, tombstone, origin_source, origin_event_id, sync_operation_id,
			        provider_ids, created_at, updated_at
			 FROM canonical_records
			 WHERE entity_type=$1 AND provider_ids ? $2`,
			entityType, provider)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []CanonicalRecord
		for rows.Next() {
			var c CanonicalRecord
			if err := rows.Scan(&c.SyncID, &c.TenantID, &c.EntityType, &c.EntityID, &c.Fields, &c.Version,
				&c.SourceVersions, &c.FieldProvenance, &c.Tombstone, &c.OriginSource, &c.OriginEventID,
				&c.SyncOperationID, &c.ProviderIDs, &c.CreatedAt, &c.UpdatedAt); err != nil {
				return nil, err
			}
			res = append(res, c)
		}
		return res, rows.Err()
	})
	return out, err
}

// reconcileDirection derives the default repair direction for a finding kind.
func reconcileDirection(kind string) string {
	switch kind {
	case FindingDeleted:
		return "delete"
	case FindingMissed:
		return "adopt_provider"
	default: // drift and missing
		return "push_canonical"
	}
}
