// Package retry implements SyncForge's durable retry machinery: the retry
// engine drains retry_queue, re-applies failed events through the worker, and
// escalates exhausted or permanent failures to the dead-letter queue.
package retry

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"syncforge/internal/backoff"
	"syncforge/internal/connectors"
	"syncforge/internal/db"
	"syncforge/internal/events"
	"syncforge/internal/observability"
	"syncforge/internal/store"
	"syncforge/internal/syncworker"
)

// Options controls retry scheduling. Defaults keep a failed event driving
// roughly 1s/2s/4s/... backoff and cap total attempts.
type Options struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
	PollEvery   time.Duration
	BatchSize   int
}

// Engine drains the durable retry queue and re-applies failed events.
type Engine struct {
	db      *db.DB
	worker  *syncworker.Worker
	log     *slog.Logger
	metrics *observability.SyncMetrics
	opts    Options
	active  atomic.Int64
}

// New builds a retry engine. Defaults: 1s base backoff, 60s cap, 8 attempts,
// poll every 250ms, batch of 100.
func New(database *db.DB, w *syncworker.Worker, metrics *observability.SyncMetrics, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.Default()
	}
	return &Engine{
		db:      database,
		worker:  w,
		log:     log,
		metrics: metrics,
		opts: Options{
			BaseDelay:   1 * time.Second,
			MaxDelay:    60 * time.Second,
			MaxAttempts: 8,
			PollEvery:   250 * time.Millisecond,
			BatchSize:   100,
		},
	}
}

// WithOptions overrides scheduling knobs (used by tests to shorten backoff).
func (e *Engine) WithOptions(o Options) *Engine {
	if o.BaseDelay > 0 {
		e.opts.BaseDelay = o.BaseDelay
	}
	if o.MaxDelay > 0 {
		e.opts.MaxDelay = o.MaxDelay
	}
	if o.MaxAttempts > 0 {
		e.opts.MaxAttempts = o.MaxAttempts
	}
	if o.PollEvery > 0 {
		e.opts.PollEvery = o.PollEvery
	}
	if o.BatchSize > 0 {
		e.opts.BatchSize = o.BatchSize
	}
	return e
}

// Run polls for due retries until ctx is cancelled. It is safe to run in a
// goroutine.
func (e *Engine) Run(ctx context.Context) error {
	ticker := time.NewTicker(e.opts.PollEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			e.drain(ctx)
		}
	}
}

// Drain processes all currently due retries once. Returns the number of retry
// rows handled. Exposed for tests.
func (e *Engine) Drain(ctx context.Context) int {
	return e.drain(ctx)
}

func (e *Engine) drain(ctx context.Context) int {
	due, err := store.ClaimDueRetries(ctx, e.db.Admin, e.opts.BatchSize)
	if err != nil {
		e.log.Error("claim due retries", "error", err)
		return 0
	}
	for i := range due {
		e.processRetry(ctx, &due[i])
	}
	return len(due)
}

func (e *Engine) processRetry(ctx context.Context, r *store.RetryEntry) {
	var ev events.Event
	if err := json.Unmarshal(r.State, &ev); err != nil {
		e.log.Error("corrupt retry state, dead-lettering", "tenant", r.TenantID, "event_id", r.EventID, "error", err)
		e.deadLetter(ctx, r, "corrupt retry state", connectors.ErrSchema.String(), r.State)
		e.discard(ctx, r)
		return
	}

	err := e.worker.Process(ctx, &ev)
	if err == nil {
		_ = store.DeleteRetry(ctx, e.db.App, r.TenantID, r.EventID)
		_ = store.ResolveDeadLetterForEvent(ctx, e.db.App, r.TenantID, r.EventID)
		e.log.Info("retry succeeded", "tenant", r.TenantID, "event_id", r.EventID, "attempts", r.Attempt+1)
		return
	}

	kind, retryAfter := connectors.Classify(err)
	if !connectors.ShouldRetry(kind) {
		e.deadLetter(ctx, r, err.Error(), kind.String(), r.State)
		e.discard(ctx, r)
		return
	}

	// failureCount is the number of processing attempts that have failed.
	failureCount := r.Attempt + 1
	if failureCount >= r.MaxAttempts {
		e.log.Warn("retries exhausted, dead-lettering", "tenant", r.TenantID, "event_id", r.EventID, "attempts", failureCount)
		e.deadLetter(ctx, r, err.Error(), kind.String(), r.State)
		e.discard(ctx, r)
		return
	}

	delay := backoff.ComputeDelay(failureCount, e.opts.BaseDelay, e.opts.MaxDelay)
	if retryAfter > 0 && retryAfter < delay {
		// Adaptive backoff: obey the provider's Retry-After where it is more
		// conservative than our own schedule.
		delay = retryAfter
	}
	if delay > e.opts.MaxDelay {
		delay = e.opts.MaxDelay
	}

	if _, err := store.EnqueueRetry(ctx, e.db.App, store.RetryEntry{
		TenantID:    r.TenantID,
		EventID:     r.EventID,
		MaxAttempts: r.MaxAttempts,
		LastError:   err.Error(),
		ErrorClass:  kind.String(),
		State:       r.State,
	}, delay); err != nil {
		e.log.Error("re-enqueue retry failed", "tenant", r.TenantID, "event_id", r.EventID, "error", err)
		return
	}
	e.metrics.RetryScheduled.Add(ctx, 1)
	e.log.Info("scheduling retry", "tenant", r.TenantID, "event_id", r.EventID, "attempt", failureCount+1, "delay", delay, "error_class", kind.String())
}

// deadLetter durably records the failure and marks the source event as dead.
func (e *Engine) deadLetter(ctx context.Context, r *store.RetryEntry, reason, errorClass string, state []byte) {
	if _, err := store.InsertDeadLetter(ctx, e.db.App, store.DeadLetter{
		TenantID:   r.TenantID,
		EventID:    r.EventID,
		Reason:     reason,
		ErrorClass: errorClass,
		Payload:    state,
	}); err != nil {
		e.log.Error("insert dead letter failed", "tenant", r.TenantID, "event_id", r.EventID, "error", err)
		return
	}
	if err := store.SetSourceEventStatusTo(ctx, e.db.App, r.TenantID, r.EventID, "dlq"); err != nil {
		e.log.Warn("mark event dlq", "event_id", r.EventID, "error", err)
	}
	e.metrics.DLQEvents.Add(ctx, 1)
}

// discard removes the retry row once an event has been permanently parked.
func (e *Engine) discard(ctx context.Context, r *store.RetryEntry) {
	if err := store.DeleteRetry(ctx, e.db.App, r.TenantID, r.EventID); err != nil {
		e.log.Warn("delete retry row", "tenant", r.TenantID, "event_id", r.EventID, "error", err)
	}
}
