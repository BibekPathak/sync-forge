// Package reconcile implements Phase 6 reconciliation: walks a provider's
// records, compares each against the canonical model, classifies divergences
// (drift, missed, deleted, missing), and either repairs them immediately
// (auto mode) or parks them as operator-reviewable findings (manual mode).
package reconcile

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"syncforge/internal/connectors"
	"syncforge/internal/connectors/registry"
	"syncforge/internal/db"
	"syncforge/internal/events"
	"syncforge/internal/observability"
	"syncforge/internal/store"
	"syncforge/internal/syncworker"
)

// Engine sweeps a provider for reconciliation. It shares the resumable
// checkpointing model of the sync job runner: cursor + counters live on the
// scheduling sync job, so a crash resumes from the last page. Findings are
// inserted idempotently per (run, kind, provider id); auto mode runs each repair
// through the worker (deterministic event id => exactly-once), manual mode
// parks findings for the operator API.
type Engine struct {
	db      *db.DB
	worker  *syncworker.Worker
	log     *slog.Logger
	metrics *observability.SyncMetrics
}

// New builds a reconciliation engine.
func New(database *db.DB, worker *syncworker.Worker, metrics *observability.SyncMetrics, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.Default()
	}
	return &Engine{db: database, worker: worker, log: log, metrics: metrics}
}

// Run executes a reconcile-type sync job end-to-end. The caller (syncjob
// runner) has already claimed the job and marked it running.
func (e *Engine) Run(ctx context.Context, job store.SyncJob) error {
	run, err := store.GetReconciliationRunByJobID(ctx, e.db.Admin, job.ID)
	if errors.Is(err, store.ErrNotFound) {
		return e.abort(ctx, job, errors.New("reconciliation run not found"))
	}
	if err != nil {
		return err
	}
	if run.Status == store.ReconcileComplete || run.Status == store.ReconcileFailed {
		e.log.Info("reconciliation already finished, skipping", "run", run.ID, "status", run.Status)
		return nil
	}

	srcConn, err := store.GetConnectionByProvider(ctx, e.db.App, job.TenantID, run.Source)
	if err != nil {
		return e.abort(ctx, job, err)
	}
	adapter, err := registry.New(srcConn.Provider, srcConn.BaseURL, "")
	if err != nil {
		return e.abort(ctx, job, err)
	}
	entityType := adapter.CanonicalEntityType()

	policies, err := store.ListPolicies(ctx, e.db.App, job.TenantID)
	if err != nil {
		return e.abort(ctx, job, err)
	}
	deletePolicy := deletePolicyFor(policies, run.Source)

	var (
		cursor     string
		processed  = job.Processed
		failed     = job.Failed
		total      = job.Total
		haveCursor = job.Cursor != nil && *job.Cursor != ""
	)
	if haveCursor {
		cursor = *job.Cursor
	}

	// Provider ids observed on this sweep; used for missing-on-provider
	// detection after the walk completes.
	seen := map[string]bool{}

	opts := connectors.ListOptions{Limit: job.BatchSize}
	for {
		if cursor != "" {
			opts.Cursor = cursor
		}
		page, err := adapter.List(ctx, opts)
		if err != nil {
			return e.abort(ctx, job, err)
		}

		for _, rec := range page.Records {
			if rec.Deleted {
				continue // already deleted at the provider; nothing to reconcile
			}
			total++
			seen[rec.ID] = true

			if err := adapter.Validate(rec); err != nil {
				failed++
				e.log.Warn("reconcile record invalid", "run", run.ID, "record", rec.ID, "error", err)
				continue
			}
			cust, err := adapter.Normalize(rec)
			if err != nil {
				failed++
				e.log.Warn("reconcile record normalize failed", "run", run.ID, "record", rec.ID, "error", err)
				continue
			}
			cust.TenantID = job.TenantID
			providerFields := cust.Fields()

			canonical, err := store.GetCanonicalByProvider(ctx, e.db.App, job.TenantID, entityType, run.Source, rec.ID)
			canonicalFound := true
			if errors.Is(err, store.ErrNotFound) {
				canonicalFound = false
			} else if err != nil {
				return e.abort(ctx, job, err)
			}

			kind := ""
			var canonicalPtr *store.CanonicalRecord
			if !canonicalFound {
				kind = store.FindingMissed
			} else {
				canonicalPtr = &canonical
				kind = classify(&canonical, providerFields)
			}
			if kind == "" {
				processed++
				continue
			}

			f, err := e.record(ctx, run, job, entityType, kind, rec, canonicalPtr, providerFields, deletePolicy)
			if err != nil {
				failed++
				e.log.Warn("reconcile record failed", "run", run.ID, "record", rec.ID, "kind", kind, "error", err)
				continue
			}
			_ = f
			processed++
		}

		nextCursor := ""
		if page.HasMore {
			nextCursor = page.NextCursor
		}
		if err := store.UpdateSyncJobProgress(ctx, e.db.Admin, job.ID, job.TenantID, nullablePtr(nextCursor), processed, failed, total); err != nil {
			return e.abort(ctx, job, err)
		}
		if !page.HasMore || nextCursor == "" {
			break
		}
		cursor = nextCursor
	}

	// Missing-on-provider detection: canonical records that map a provider id
	// we never saw on the sweep are absent from the provider. Auto mode may
	// recreate them only when the tenant's delete policy allows it.
	canonicals, err := store.ListCanonicalByProvider(ctx, e.db.App, job.TenantID, entityType, run.Source)
	if err != nil {
		return e.abort(ctx, job, err)
	}
	for _, c := range canonicals {
		if c.Tombstone {
			continue // tombstoned locally: absence on the provider is correct
		}
		providerID := c.ProviderIDs[run.Source]
		if providerID == "" || seen[providerID] {
			continue
		}
		f, err := e.record(ctx, run, job, entityType, store.FindingMissing,
			connectors.ProviderRecord{ID: providerID, SourceVersion: c.SourceVersions[run.Source]},
			&c, c.Fields, deletePolicy)
		if err != nil {
			e.log.Warn("reconcile missing failed", "run", run.ID, "entity", c.EntityID, "error", err)
		}
		_ = f
	}

	drift, missed, deleted, err := store.CountReconciliationFindings(ctx, e.db.App, job.TenantID, run.ID)
	if err != nil {
		return e.abort(ctx, job, err)
	}
	if err := store.UpdateReconciliationCounters(ctx, e.db.Admin, run.ID, total, drift, missed, deleted); err != nil {
		return e.abort(ctx, job, err)
	}
	if err := store.FinishReconciliationRun(ctx, e.db.Admin, run.ID, store.ReconcileComplete, nil); err != nil {
		return e.abort(ctx, job, err)
	}
	e.metrics.ReconcileRuns.Add(ctx, 1)
	e.log.Info("reconciliation completed", "run", run.ID, "tenant", job.TenantID,
		"mode", run.Mode, "records", total, "drift", drift, "missed", missed, "deleted", deleted)
	return store.FinishSyncJob(ctx, e.db.Admin, job.ID, job.TenantID, "completed", nil)
}

// record classifies, persists, and (in auto mode) repairs a single divergence.
// Findings deduplicate on (run_id, kind, provider_id); a re-sweep after a crash
// returns the existing row and leaves an operator's decision intact.
func (e *Engine) record(ctx context.Context, run store.ReconciliationRun, job store.SyncJob, entityType string,
	kind string, rec connectors.ProviderRecord, canonical *store.CanonicalRecord,
	providerFields map[string]any, deletePolicy string) (store.ReconciliationFinding, error) {

	f := store.ReconciliationFinding{
		RunID:           run.ID,
		TenantID:        job.TenantID,
		Kind:            kind,
		ProviderID:      rec.ID,
		ProviderFields:  providerFields,
		ProviderVersion: rec.SourceVersion,
	}
	if canonical != nil {
		f.CanonicalID = canonical.EntityID
		f.CanonicalFields = canonical.Fields
	}
	stored, err := store.InsertReconciliationFinding(ctx, e.db.App, f)
	if err != nil {
		return store.ReconciliationFinding{}, err
	}
	e.metrics.ReconcileFindings.Add(ctx, 1)

	if run.Mode == "auto" && stored.Status == store.FindingPending && e.repairable(stored, deletePolicy) {
		if err := e.repair(ctx, run, job, entityType, stored); err != nil {
			e.log.Warn("reconcile repair failed", "run", run.ID, "finding", stored.ID, "direction", stored.Direction, "error", err)
		}
	} else if run.Mode == "auto" && stored.Status == store.FindingPending {
		// Policy blocks the repair (delete propagation or re-creation): park it
		// as skipped so operators see why nothing was done.
		reason := policyGateReason(stored.Kind, deletePolicy)
		if err := store.SetReconciliationFindingStatus(ctx, e.db.App, job.TenantID, stored.ID, store.FindingSkipped, &reason); err != nil {
			e.log.Warn("mark reconcile finding skipped", "finding", stored.ID, "error", err)
		}
	}
	return stored, nil
}

// repairable reports whether auto mode may act on a finding under the source's
// delete policy. The deleted direction is only applied when deletes propagate;
// the missing direction (re-creating an absent provider record) only when the
// policy permits resurrecting external deletions.
func (e *Engine) repairable(f store.ReconciliationFinding, deletePolicy string) bool {
	switch f.Kind {
	case store.FindingDeleted:
		return deletePolicy == "propagate"
	case store.FindingMissing:
		return shouldRecreateMissing(deletePolicy)
	default:
		return true
	}
}

func policyGateReason(kind, deletePolicy string) string {
	if kind == store.FindingDeleted {
		return "delete_policy=" + deletePolicy + ": deletes do not propagate"
	}
	return "delete_policy=" + deletePolicy + ": external deletions are respected"
}

// repair routes a finding through the worker as a deterministic reconcile
// event. The worker's idempotency claim (tenant, source, event_id) makes a
// crashed-and-resumed run exactly-once.
func (e *Engine) repair(ctx context.Context, run store.ReconciliationRun, job store.SyncJob, entityType string, f store.ReconciliationFinding) error {
	ev := events.Event{
		EventID:       "reconcile:" + f.ID,
		TenantID:      job.TenantID,
		Source:        run.Source,
		EntityType:    entityType,
		EntityID:      f.ProviderID,
		EventType:     events.EventUpdated,
		SourceVersion: f.ProviderVersion,
		OccurredAt:    time.Now().UTC(),
		ReceivedAt:    time.Now().UTC(),
		Provenance:    events.Provenance{ReconcileFindingID: f.ID},
		Payload:       map[string]any{},
	}
	return e.worker.Process(ctx, &ev)
}

// abort marks both the reconciliation run and its sync job failed.
func (e *Engine) abort(ctx context.Context, job store.SyncJob, cause error) error {
	msg := cause.Error()
	e.log.Error("reconciliation aborted", "job", job.ID, "tenant", job.TenantID, "error", cause)
	if run, err := store.GetReconciliationRunByJobID(ctx, e.db.Admin, job.ID); err == nil {
		_ = store.FinishReconciliationRun(ctx, e.db.Admin, run.ID, store.ReconcileFailed, &msg)
	}
	_ = store.FinishSyncJob(ctx, e.db.Admin, job.ID, job.TenantID, "failed", &msg)
	return cause
}

// deletePolicyFor returns the delete policy governing a source: the first
// policy mentioning the source (as source or destination), defaulting to
// 'propagate' when there is none.
func deletePolicyFor(policies []store.SyncPolicy, source string) string {
	for _, p := range policies {
		if p.Source == source {
			return p.DeletePolicy
		}
	}
	for _, p := range policies {
		if p.Destination == source {
			return p.DeletePolicy
		}
	}
	return "propagate"
}

func nullablePtr(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}
