package syncworker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"

	"syncforge/internal/backoff"
	"syncforge/internal/connectors"
	"syncforge/internal/connectors/registry"
	"syncforge/internal/db"
	"syncforge/internal/events"
	"syncforge/internal/model"
	"syncforge/internal/observability"
	"syncforge/internal/store"
)

// Options tunes worker reliability behavior.
type Options struct {
	// RetryBaseDelay/RetryMaxDelay shape the exponential backoff used when a
	// failure is first handed to the durable retry machinery.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// RetryMaxAttempts caps total processing attempts before an event is
	// escalated to the dead-letter queue.
	RetryMaxAttempts int
}

// Worker consumes canonical sync events, applies them to destination systems,
// and persists canonical state. Phase 3 adds identity resolution, per-source
// version checks (out-of-order protection) and fingerprint-based loop
// prevention for bidirectional synchronization. Phase 4 routes failures to the
// durable retry queue / dead-letter queue instead of relying on broker
// redelivery.
type Worker struct {
	db      *db.DB
	log     *slog.Logger
	metrics *observability.SyncMetrics
	opts    Options
}

func New(database *db.DB, metrics *observability.SyncMetrics, log *slog.Logger) *Worker {
	if log == nil {
		log = slog.Default()
	}
	return &Worker{
		db:      database,
		metrics: metrics,
		log:     log,
		opts: Options{
			RetryBaseDelay:   1 * time.Second,
			RetryMaxDelay:    60 * time.Second,
			RetryMaxAttempts: 8,
		},
	}
}

// WithOptions overrides worker reliability knobs.
func (w *Worker) WithOptions(o Options) *Worker {
	if o.RetryBaseDelay > 0 {
		w.opts.RetryBaseDelay = o.RetryBaseDelay
	}
	if o.RetryMaxDelay > 0 {
		w.opts.RetryMaxDelay = o.RetryMaxDelay
	}
	if o.RetryMaxAttempts > 0 {
		w.opts.RetryMaxAttempts = o.RetryMaxAttempts
	}
	return w
}

// Handle implements eventbus.Handler. A processing failure is acknowledged
// (nil) only after it has been made durable in the retry queue or DLQ; this
// keeps broker semantics at at-least-once while the durable machinery owns the
// actual retries with backoff.
func (w *Worker) Handle(ctx context.Context, _ string, value []byte) error {
	var ev events.Event
	if err := json.Unmarshal(value, &ev); err != nil {
		return err
	}
	if err := w.Process(ctx, &ev); err != nil {
		if err := w.dispatchFailure(ctx, &ev, err); err != nil {
			// Failure could not be made durable: return the error so the
			// transport redelivers the message later.
			return err
		}
		return nil
	}
	return nil
}

// Process applies a single canonical event. It is idempotent: claiming the
// event in processed_events before doing work means duplicate deliveries are
// no-ops.
func (w *Worker) Process(ctx context.Context, ev *events.Event) error {
	start := time.Now()
	w.metrics.EventsTotal.Add(ctx, 1)

	claimed, err := store.ClaimProcessedEvent(ctx, w.db.App, ev.TenantID, ev.Source, ev.EventID, ev.EntityType, ev.EntityID)
	if err != nil {
		w.metrics.EventsFailed.Add(ctx, 1)
		return err
	}
	if !claimed {
		// Duplicate delivery: the logical event was already applied.
		w.metrics.Duplicates.Add(ctx, 1)
		w.log.Info("duplicate event skipped", "event_id", ev.EventID)
		return nil
	}

	policies, err := store.ListPolicies(ctx, w.db.App, ev.TenantID)
	if err != nil {
		w.fail(ctx, ev, err)
		return err
	}

	// Normalize the provider's entity type to the canonical one via the source
	// adapter (e.g. hubspot "contact" -> canonical "customer").
	entityType, err := w.canonicalEntityType(ctx, ev)
	if err != nil {
		w.fail(ctx, ev, err)
		return err
	}

	var targets []store.SyncPolicy
	for _, p := range policies {
		if p.Source == ev.Source && p.Entity == entityType {
			targets = append(targets, p)
		}
	}
	if len(targets) == 0 {
		w.log.Info("no policy for event, releasing claim", "event_id", ev.EventID, "source", ev.Source)
		_ = store.ReleaseProcessedEvent(ctx, w.db.App, ev.TenantID, ev.Source, ev.EventID)
		_ = store.SetSourceEventStatus(ctx, w.db.App, ev.TenantID, ev.EventID, "validated", "processed")
		return nil
	}

	if ev.EventType == events.EventDeleted {
		err = w.applyDelete(ctx, ev, entityType, targets)
	} else {
		err = w.applyUpsert(ctx, ev, entityType, targets)
	}
	if err != nil {
		w.fail(ctx, ev, err)
		return err
	}

	if err := store.SetSourceEventStatus(ctx, w.db.App, ev.TenantID, ev.EventID, "validated", "processed"); err != nil {
		w.log.Warn("mark event processed", "event_id", ev.EventID, "error", err)
	}

	w.metrics.EventsSuccess.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(ev.Source)))
	w.metrics.ProcessingDuration.Record(ctx, time.Since(start).Seconds())
	w.log.Info("event processed",
		"event_id", ev.EventID, "source", ev.Source, "entity", ev.EntityID, "type", ev.EventType)
	return nil
}

// fail marks an event as failed and releases its idempotency claim so the
// durable retry machinery (Phase 4) can re-run it.
func (w *Worker) fail(ctx context.Context, ev *events.Event, cause error) {
	w.metrics.EventsFailed.Add(ctx, 1)
	_ = store.ReleaseProcessedEvent(ctx, w.db.App, ev.TenantID, ev.Source, ev.EventID)
	if err := store.SetSourceEventStatus(ctx, w.db.App, ev.TenantID, ev.EventID, "validated", "failed"); err != nil {
		w.log.Warn("mark event failed", "event_id", ev.EventID, "error", err)
	}
	w.log.Error("event failed",
		"event_id", ev.EventID, "source", ev.Source, "entity", ev.EntityID, "error", cause)
}

// dispatchFailure makes a failed event durable: transient/retryable failures
// are scheduled on the retry queue with exponential backoff; permanent
// failures (schema, auth) go straight to the dead-letter queue. Returns an
// error only if the handoff itself could not be persisted.
func (w *Worker) dispatchFailure(ctx context.Context, ev *events.Event, cause error) error {
	kind, retryAfter := connectors.Classify(cause)
	if !connectors.ShouldRetry(kind) {
		return w.deadLetter(ctx, ev, cause.Error(), kind.String())
	}

	delay := backoff.ComputeDelay(1, w.opts.RetryBaseDelay, w.opts.RetryMaxDelay)
	if retryAfter > 0 && retryAfter < delay {
		delay = retryAfter
	}
	if delay > w.opts.RetryMaxDelay {
		delay = w.opts.RetryMaxDelay
	}

	state, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if _, err := store.EnqueueRetry(ctx, w.db.App, store.RetryEntry{
		TenantID:    ev.TenantID,
		EventID:     ev.EventID,
		MaxAttempts: w.opts.RetryMaxAttempts,
		LastError:   cause.Error(),
		ErrorClass:  kind.String(),
		State:       state,
	}, delay); err != nil {
		return err
	}
	w.metrics.RetryScheduled.Add(ctx, 1)
	w.log.Info("scheduled retry", "event_id", ev.EventID, "source", ev.Source, "delay", delay, "error_class", kind.String())
	return nil
}

// deadLetter parks a permanently-failed event for operator inspection.
func (w *Worker) deadLetter(ctx context.Context, ev *events.Event, reason, errorClass string) error {
	state, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if _, err := store.InsertDeadLetter(ctx, w.db.App, store.DeadLetter{
		TenantID:   ev.TenantID,
		EventID:    ev.EventID,
		Reason:     reason,
		ErrorClass: errorClass,
		Payload:    state,
	}); err != nil {
		return err
	}
	if err := store.SetSourceEventStatusTo(ctx, w.db.App, ev.TenantID, ev.EventID, "dlq"); err != nil {
		w.log.Warn("mark event dlq", "event_id", ev.EventID, "error", err)
	}
	w.metrics.DLQEvents.Add(ctx, 1)
	w.log.Error("event dead-lettered",
		"event_id", ev.EventID, "source", ev.Source, "error_class", errorClass, "reason", reason)
	return nil
}

// canonicalEntityType resolves the source adapter and returns the canonical
// entity type for the event's provider entity type.
func (w *Worker) canonicalEntityType(ctx context.Context, ev *events.Event) (string, error) {
	srcConn, err := store.GetConnectionByProvider(ctx, w.db.App, ev.TenantID, ev.Source)
	if err != nil {
		return "", err
	}
	srcAdapter, err := registry.New(srcConn.Provider, srcConn.BaseURL, "")
	if err != nil {
		return "", err
	}
	return srcAdapter.CanonicalEntityType(), nil
}

// applyUpsert handles created/updated events: identity resolution, version
// check, loop-prevention, then propagation to every configured destination.
func (w *Worker) applyUpsert(ctx context.Context, ev *events.Event, entityType string, policies []store.SyncPolicy) error {
	tenant := ev.TenantID
	fields, ok := ev.Payload["fields"].(map[string]any)
	if !ok || len(fields) == 0 {
		return connectors.NewError(connectors.ErrSchema, "event payload missing fields", nil)
	}
	srcConn, err := store.GetConnectionByProvider(ctx, w.db.App, tenant, ev.Source)
	if err != nil {
		return err
	}
	srcAdapter, err := registry.New(srcConn.Provider, srcConn.BaseURL, "")
	if err != nil {
		return err
	}

	srcRec := connectors.ProviderRecord{ID: ev.EntityID, SourceVersion: ev.SourceVersion, Data: fields}
	if err := srcAdapter.Validate(srcRec); err != nil {
		return err
	}
	cust, err := srcAdapter.Normalize(srcRec)
	if err != nil {
		return err
	}
	cust.TenantID = tenant

	// 1. Identity resolution: map this provider record to a canonical entity
	//    (by provider id, then by email).
	canonical, err := w.resolveCanonical(ctx, ev, entityType, cust)
	if err != nil {
		return err
	}

	// 2. Ordering: drop out-of-order (stale) events from this source.
	if ev.SourceVersion <= canonical.SourceVersions[ev.Source] {
		w.metrics.StaleEvents.Add(ctx, 1)
		w.log.Info("stale event dropped",
			"event_id", ev.EventID, "source", ev.Source,
			"incoming_version", ev.SourceVersion, "applied_version", canonical.SourceVersions[ev.Source])
		return nil
	}

	// 3. Loop prevention: if this event is exactly what we last wrote to this
	//    source, it is our own echo — do not propagate it back.
	echo, err := w.isEcho(ctx, tenant, entityType, canonical.EntityID, ev.Source, cust)
	if err != nil {
		return err
	}
	if echo {
		w.metrics.LoopsPrevented.Add(ctx, 1)
		w.log.Info("loop prevented (own write echo)", "event_id", ev.EventID, "source", ev.Source, "entity", canonical.EntityID)
		return nil
	}

	// 4. Propagate to every destination.
	for _, policy := range policies {
		dstConn, err := store.GetConnectionByProvider(ctx, w.db.App, tenant, policy.Destination)
		if err != nil {
			return err
		}
		dstAdapter, err := registry.New(dstConn.Provider, dstConn.BaseURL, "")
		if err != nil {
			return err
		}
		dstRec, err := dstAdapter.Denormalize(cust)
		if err != nil {
			return err
		}

		dstID := canonical.ProviderIDs[policy.Destination]
		if dstID == "" {
			created, err := dstAdapter.Create(ctx, dstRec)
			if err != nil {
				return err
			}
			canonical.ProviderIDs[policy.Destination] = created.ID
		} else {
			if _, err := dstAdapter.Update(ctx, dstID, dstRec); err != nil {
				return err
			}
		}
		w.metrics.DestinationWrites.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(policy.Destination)))

		// Record what we wrote so its echo is recognized and dropped later.
		if err := store.UpsertOutboundWrite(ctx, w.db.App, store.OutboundWrite{
			TenantID:     tenant,
			EntityType:   entityType,
			EntityID:     canonical.EntityID,
			TargetSource: policy.Destination,
			Fingerprint:  cust.Fingerprint(),
		}); err != nil {
			return err
		}
	}

	// 5. Persist canonical state.
	canonical.Fields = cust.Fields()
	canonical.SourceVersions[ev.Source] = ev.SourceVersion
	canonical.Version++
	canonical.OriginSource = ev.Source
	canonical.OriginEventID = ev.EventID
	canonical.Tombstone = false
	_, err = store.UpsertCanonical(ctx, w.db.App, canonical)
	return err
}

// applyDelete handles deletes: propagate to destinations and retain a tombstone.
// A tombstoned canonical short-circuits further deletes, which breaks delete
// echo loops (our delete causes a destination delete webhook).
func (w *Worker) applyDelete(ctx context.Context, ev *events.Event, entityType string, policies []store.SyncPolicy) error {
	tenant := ev.TenantID
	canonical, err := store.GetCanonicalByProvider(ctx, w.db.App, tenant, entityType, ev.Source, ev.EntityID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if canonical.Tombstone {
		w.log.Info("delete ignored: entity already tombstoned", "event_id", ev.EventID, "entity", canonical.EntityID)
		return nil
	}

	for _, policy := range policies {
		if policy.DeletePolicy != "propagate" {
			continue
		}
		dstID := canonical.ProviderIDs[policy.Destination]
		if dstID == "" {
			continue
		}
		dstConn, err := store.GetConnectionByProvider(ctx, w.db.App, tenant, policy.Destination)
		if err != nil {
			return err
		}
		dstAdapter, err := registry.New(dstConn.Provider, dstConn.BaseURL, "")
		if err != nil {
			return err
		}
		if err := dstAdapter.Delete(ctx, dstID); err != nil {
			return err
		}
		w.metrics.DestinationWrites.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(policy.Destination)))
	}

	canonical.Tombstone = true
	canonical.SourceVersions[ev.Source] = ev.SourceVersion
	canonical.Version++
	canonical.OriginSource = ev.Source
	canonical.OriginEventID = ev.EventID
	_, err = store.UpsertCanonical(ctx, w.db.App, canonical)
	return err
}

// resolveCanonical maps an incoming provider record to a canonical entity:
// by provider id, then by email (identity resolution), else creates a new one.
func (w *Worker) resolveCanonical(ctx context.Context, ev *events.Event, entityType string, cust *model.Customer) (store.CanonicalRecord, error) {
	tenant := ev.TenantID
	canonical, err := store.GetCanonicalByProvider(ctx, w.db.App, tenant, entityType, ev.Source, ev.EntityID)
	if err == nil {
		return canonical, nil
	}
	if err != store.ErrNotFound {
		return store.CanonicalRecord{}, err
	}

	if cust.Email != "" {
		canonical, err = store.GetCanonicalByEmail(ctx, w.db.App, tenant, entityType, cust.Email)
		if err == nil {
			// Link this provider's record id to the matched canonical entity so
			// future events resolve directly.
			canonical.ProviderIDs[ev.Source] = ev.EntityID
			if canonical.SourceVersions == nil {
				canonical.SourceVersions = map[string]int64{}
			}
			if err := store.AddProviderID(ctx, w.db.App, tenant, entityType, canonical.EntityID, ev.Source, ev.EntityID); err != nil {
				return store.CanonicalRecord{}, err
			}
			w.log.Info("identity resolved by email",
				"source", ev.Source, "provider_id", ev.EntityID, "entity_id", canonical.EntityID, "email", cust.Email)
			return canonical, nil
		}
		if err != store.ErrNotFound {
			return store.CanonicalRecord{}, err
		}
	}

	return store.CanonicalRecord{
		TenantID:        tenant,
		EntityType:      entityType,
		EntityID:        ev.EntityID,
		Version:         0,
		ProviderIDs:     map[string]string{ev.Source: ev.EntityID},
		SourceVersions:  map[string]int64{},
		FieldProvenance: map[string]any{},
	}, nil
}

// isEcho reports whether an incoming event from source is SyncForge's own
// write echoed back, by comparing the incoming canonical fingerprint against
// the fingerprint we last recorded for that (entity, source).
func (w *Worker) isEcho(ctx context.Context, tenant, entityType, entityID, source string, cust *model.Customer) (bool, error) {
	outbound, err := store.GetOutboundWrite(ctx, w.db.App, tenant, entityType, entityID, source)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return outbound.Fingerprint == cust.Fingerprint(), nil
}
