package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// SyncJob is a resumable synchronization run: an initial full sync or a
// reconciliation sweep. Cursor + counters are the checkpoint: a worker that
// crashes can resume from the last persisted cursor instead of restarting from
// zero.
type SyncJob struct {
	ID          string     `json:"id"`
	TenantID    string     `json:"tenant_id"`
	Entity      string     `json:"entity"`
	Source      string     `json:"source"`
	Destination string     `json:"destination"`
	Type        string     `json:"type"`
	Status      string     `json:"status"`
	Cursor      *string    `json:"cursor,omitempty"`
	Processed   int64      `json:"processed"`
	Failed      int64      `json:"failed"`
	Total       int64      `json:"total"`
	BatchSize   int        `json:"batch_size"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	LastError   *string    `json:"last_error,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
}

// CreateSyncJob records a pending synchronization job for a tenant.
func CreateSyncJob(ctx context.Context, pool *pgxpool.Pool, j SyncJob) (SyncJob, error) {
	if j.Type == "" {
		j.Type = "initial"
	}
	if j.BatchSize <= 0 {
		j.BatchSize = 1000
	}
	var out SyncJob
	_, err := db.WithTenant[struct{}](ctx, pool, j.TenantID, func(tx pgx.Tx) (struct{}, error) {
		return struct{}{}, tx.QueryRow(ctx,
			`INSERT INTO sync_jobs (tenant_id, entity, source, destination, type, status, batch_size)
			 VALUES ($1,$2,$3,$4,$5,'pending',$6)
			 RETURNING id, tenant_id, entity, source, destination, type, status, cursor,
			           processed, failed, total, batch_size, started_at, finished_at, last_error, created_at`,
			j.TenantID, j.Entity, j.Source, j.Destination, j.Type, j.BatchSize,
		).Scan(&out.ID, &out.TenantID, &out.Entity, &out.Source, &out.Destination, &out.Type,
			&out.Status, &out.Cursor, &out.Processed, &out.Failed, &out.Total,
			&out.BatchSize, &out.StartedAt, &out.FinishedAt, &out.LastError, &out.CreatedAt)
	})
	if err != nil {
		return SyncJob{}, err
	}
	return out, nil
}

// GetSyncJob fetches a job within a tenant.
func GetSyncJob(ctx context.Context, pool *pgxpool.Pool, tenantID, id string) (SyncJob, error) {
	out, err := db.WithTenant[SyncJob](ctx, pool, tenantID, func(tx pgx.Tx) (SyncJob, error) {
		var j SyncJob
		err := tx.QueryRow(ctx,
			`SELECT id, tenant_id, entity, source, destination, type, status, cursor,
			        processed, failed, total, batch_size, started_at, finished_at, last_error, created_at
			 FROM sync_jobs WHERE id=$1`, id,
		).Scan(&j.ID, &j.TenantID, &j.Entity, &j.Source, &j.Destination, &j.Type, &j.Status,
			&j.Cursor, &j.Processed, &j.Failed, &j.Total, &j.BatchSize,
			&j.StartedAt, &j.FinishedAt, &j.LastError, &j.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return SyncJob{}, ErrNotFound
		}
		return j, err
	})
	return out, err
}

// ListSyncJobs returns a tenant's jobs, newest first.
func ListSyncJobs(ctx context.Context, pool *pgxpool.Pool, tenantID string, limit int) ([]SyncJob, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	out, err := db.WithTenant[[]SyncJob](ctx, pool, tenantID, func(tx pgx.Tx) ([]SyncJob, error) {
		rows, err := tx.Query(ctx,
			`SELECT id, tenant_id, entity, source, destination, type, status, cursor,
			        processed, failed, total, batch_size, started_at, finished_at, last_error, created_at
			 FROM sync_jobs ORDER BY created_at DESC LIMIT $1`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []SyncJob
		for rows.Next() {
			var j SyncJob
			if err := rows.Scan(&j.ID, &j.TenantID, &j.Entity, &j.Source, &j.Destination, &j.Type,
				&j.Status, &j.Cursor, &j.Processed, &j.Failed, &j.Total, &j.BatchSize,
				&j.StartedAt, &j.FinishedAt, &j.LastError, &j.CreatedAt); err != nil {
				return nil, err
			}
			res = append(res, j)
		}
		return res, rows.Err()
	})
	return out, err
}

// ClaimNextSyncJob atomically claims the next runnable job (pending/resumed, or
// a stale running job whose worker crashed) and marks it running. Cross-tenant
// (admin pool) because the runner serves all tenants.
func ClaimNextSyncJob(ctx context.Context, pool *pgxpool.Pool) (SyncJob, error) {
	var j SyncJob
	rows, err := pool.Query(ctx,
		`SELECT id, tenant_id, entity, source, destination, type, status, cursor,
		        processed, failed, total, batch_size, started_at, finished_at, last_error, created_at
		 FROM sync_jobs
		 WHERE status IN ('pending','paused')
		    OR (status='running' AND started_at < now() - interval '60 seconds')
		 ORDER BY created_at
		 LIMIT 1
		 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return SyncJob{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return SyncJob{}, ErrNotFound
	}
	if err := rows.Scan(&j.ID, &j.TenantID, &j.Entity, &j.Source, &j.Destination, &j.Type,
		&j.Status, &j.Cursor, &j.Processed, &j.Failed, &j.Total, &j.BatchSize,
		&j.StartedAt, &j.FinishedAt, &j.LastError, &j.CreatedAt); err != nil {
		return SyncJob{}, err
	}
	if err := rows.Err(); err != nil {
		return SyncJob{}, err
	}

	_, err = pool.Exec(ctx,
		`UPDATE sync_jobs SET status='running', started_at=COALESCE(started_at, now())
		 WHERE id=$1 AND tenant_id=$2`, j.ID, j.TenantID)
	if err != nil {
		return SyncJob{}, err
	}
	j.Status = "running"
	now := time.Now()
	if j.StartedAt == nil {
		j.StartedAt = &now
	}
	return j, nil
}

// UpdateSyncJobProgress persists a checkpoint. The admin pool is used so the
// runner can checkpoint across tenants; status is left untouched here.
func UpdateSyncJobProgress(ctx context.Context, pool *pgxpool.Pool, id, tenantID string, cursor *string, processed, failed, total int64) error {
	_, err := pool.Exec(ctx,
		`UPDATE sync_jobs SET cursor=$3, processed=$4, failed=$5, total=$6
		 WHERE id=$1 AND tenant_id=$2`,
		id, tenantID, cursor, processed, failed, total)
	return err
}

// FinishSyncJob marks a job completed or failed and clears the running flag.
func FinishSyncJob(ctx context.Context, pool *pgxpool.Pool, id, tenantID, status string, lastError *string) error {
	_, err := pool.Exec(ctx,
		`UPDATE sync_jobs SET status=$3, finished_at=now(), last_error=$4
		 WHERE id=$1 AND tenant_id=$2`,
		id, tenantID, status, lastError)
	return err
}
