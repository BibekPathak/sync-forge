// Package syncjob implements resumable initial full synchronization. It
// streams a provider's records page by page, applies each through the same
// idempotent worker used by webhook events, and checkpoints (cursor + counters)
// after every page so a crashing worker resumes instead of restarting from
// zero. Because events use deterministic ids and the worker claims each logical
// event before doing work, re-processing an already-applied page is a no-op.
package syncjob

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"syncforge/internal/connectors"
	"syncforge/internal/connectors/registry"
	"syncforge/internal/db"
	"syncforge/internal/events"
	"syncforge/internal/reconcile"
	"syncforge/internal/store"
	"syncforge/internal/syncworker"
)

// Runner claims pending sync jobs and executes them, resuming on crash.
type Runner struct {
	db         *db.DB
	worker     *syncworker.Worker
	reconciler *reconcile.Engine
	log        *slog.Logger
	pollEvery  time.Duration
}

// New builds a sync job runner.
func New(database *db.DB, worker *syncworker.Worker, log *slog.Logger) *Runner {
	if log == nil {
		log = slog.Default()
	}
	return &Runner{db: database, worker: worker, log: log, pollEvery: 2 * time.Second}
}

// WithReconciler attaches the reconciliation engine used for reconcile-type
// jobs. Without it, reconcile jobs are aborted as misconfigured.
func (r *Runner) WithReconciler(rec *reconcile.Engine) *Runner {
	r.reconciler = rec
	return r
}

// Run polls for runnable jobs until ctx is cancelled. Each job processes
// synchronously; jobs for different tenants are serialized by the single
// runner worker.
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.pollEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			job, err := store.ClaimNextSyncJob(ctx, r.db.Admin)
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				r.log.Error("claim sync job", "error", err)
				continue
			}
			if err := r.execute(ctx, job); err != nil {
				r.log.Error("sync job failed", "job", job.ID, "tenant", job.TenantID, "error", err)
			}
		}
	}
}

// Execute runs a single job to completion. Exposed for tests and the demo
// script so a full sync can be driven without waiting for the poll loop.
func (r *Runner) Execute(ctx context.Context, job store.SyncJob) error {
	return r.execute(ctx, job)
}

func (r *Runner) execute(ctx context.Context, job store.SyncJob) error {
	r.log.Info("sync job starting", "job", job.ID, "tenant", job.TenantID,
		"source", job.Source, "destination", job.Destination, "cursor", nullable(job.Cursor))

	// Reconcile-type jobs delegate to the reconciliation engine, which owns
	// its own lifecycle (findings, counters, finish) for the run.
	if job.Type == "reconcile" {
		if r.reconciler == nil {
			return r.abort(ctx, job, errors.New("reconcile engine not configured"))
		}
		return r.reconciler.Run(ctx, job)
	}

	srcConn, err := store.GetConnectionByProvider(ctx, r.db.App, job.TenantID, job.Source)
	if err != nil {
		return r.abort(ctx, job, err)
	}
	adapter, err := registry.New(srcConn.Provider, srcConn.BaseURL, "")
	if err != nil {
		return r.abort(ctx, job, err)
	}
	dstConn, err := store.GetConnectionByProvider(ctx, r.db.App, job.TenantID, job.Destination)
	if err != nil {
		return r.abort(ctx, job, err)
	}
	if _, err := registry.New(dstConn.Provider, dstConn.BaseURL, ""); err != nil {
		return r.abort(ctx, job, err)
	}

	// Size the progress bar with the provider's reported record count when
	// available; otherwise grow Total as we discover records.
	total := job.Total
	if h, err := adapter.HealthCheck(ctx); err == nil && h.Records > 0 {
		total = h.Records
	}

	var (
		cursor     string
		processed  = job.Processed
		failed     = job.Failed
		haveCursor = job.Cursor != nil && *job.Cursor != ""
	)
	if haveCursor {
		cursor = *job.Cursor
	}

	opts := connectors.ListOptions{Limit: job.BatchSize}
	for {
		if cursor != "" {
			opts.Cursor = cursor
		}
		page, err := adapter.List(ctx, opts)
		if err != nil {
			return r.abort(ctx, job, err)
		}

		// Apply the page. Idempotency claims make re-visiting a crashed page
		// safe: already-applied records are skipped.
		for _, rec := range page.Records {
			if rec.Deleted {
				continue // full sync does not backfill deletions
			}
			if err := adapter.Validate(rec); err != nil {
				// Malformed record: dead-letter it without crashing the run.
				failed++
				r.deadLetterProviderRecord(ctx, job, rec, err)
				continue
			}
			ev := events.Event{
				EventID:       jobEventID(job.ID, rec.ID),
				TenantID:      job.TenantID,
				Source:        job.Source,
				EntityType:    adapter.CanonicalEntityType(),
				EntityID:      rec.ID,
				EventType:     events.EventCreated,
				SourceVersion: rec.SourceVersion,
				OccurredAt:    time.Now().UTC(),
				ReceivedAt:    time.Now().UTC(),
				Provenance:    events.Provenance{OriginSource: job.Source, SyncOperationID: job.ID},
				Payload:       map[string]any{"fields": rec.Data},
			}
			pErr := r.worker.Process(ctx, &ev)
			if pErr != nil {
				failed++
				r.log.Warn("full sync record failed", "job", job.ID, "record", rec.ID, "error", pErr)
				continue
			}
			processed++
		}

		// Checkpoint after the page so we resiliently resume on crash.
		nextCursor := ""
		if page.HasMore {
			nextCursor = page.NextCursor
		}
		if err := store.UpdateSyncJobProgress(ctx, r.db.Admin, job.ID, job.TenantID, nullablePtr(nextCursor), processed, failed, total); err != nil {
			return r.abort(ctx, job, err)
		}
		r.log.Info("sync job checkpoint", "job", job.ID, "processed", processed, "failed", failed, "total", total, "cursor", nextCursor)

		if !page.HasMore {
			break
		}
		// Fresh page for the loop so SetOption doesn't reuse a stale cursor.
		if nextCursor == "" {
			break
		}
		cursor = nextCursor
	}

	return store.FinishSyncJob(ctx, r.db.Admin, job.ID, job.TenantID, "completed", nil)
}

// abort marks a job failed and returns the cause. The job stays available for
// a future run (re-claimable) since status 'failed' is not claimable; an
// operator may reopen it via the API.
func (r *Runner) abort(ctx context.Context, job store.SyncJob, cause error) error {
	rErr := cause.Error()
	r.log.Error("sync job aborted", "job", job.ID, "tenant", job.TenantID, "error", cause)
	_ = store.FinishSyncJob(ctx, r.db.Admin, job.ID, job.TenantID, "failed", &rErr)
	return cause
}

// deadLetterProviderRecord parks a malformed provider record from a full sync.
// The payload is a well-formed canonical event so an operator retry is intact;
// re-processing still fails schema validation and re-parks it.
func (r *Runner) deadLetterProviderRecord(ctx context.Context, job store.SyncJob, rec connectors.ProviderRecord, cause error) {
	ev := events.Event{
		EventID:       jobEventID(job.ID, rec.ID),
		TenantID:      job.TenantID,
		Source:        job.Source,
		EntityType:    "customer",
		EntityID:      rec.ID,
		EventType:     events.EventCreated,
		SourceVersion: rec.SourceVersion,
		ReceivedAt:    time.Now().UTC(),
		Payload:       map[string]any{"fields": rec.Data},
	}
	state, err := json.Marshal(ev)
	if err != nil {
		r.log.Warn("marshal full sync record", "job", job.ID, "record", rec.ID, "error", err)
		return
	}
	if _, err := store.InsertDeadLetter(ctx, r.db.App, store.DeadLetter{
		TenantID:   job.TenantID,
		EventID:    ev.EventID,
		Reason:     cause.Error(),
		ErrorClass: connectors.ErrSchema.String(),
		Payload:    state,
	}); err != nil {
		r.log.Warn("dead-letter full sync record", "job", job.ID, "record", rec.ID, "error", err)
	}
	r.log.Warn("full sync record dead-lettered", "job", job.ID, "record", rec.ID, "reason", cause)
}

// jobEventID is the deterministic idempotency key for a full-sync record. The
// same job resumed produces the same ids, so the worker's claim deduplicates
// re-processed pages.
func jobEventID(jobID, recordID string) string {
	return "jobsync:" + jobID + ":" + recordID
}

func nullable(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func nullablePtr(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
