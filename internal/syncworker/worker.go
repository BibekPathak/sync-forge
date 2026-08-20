package syncworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"syncforge/internal/backoff"
	"syncforge/internal/conflict"
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
	// ConnectorTimeout bounds provider API calls. Zero means the registry
	// default. Shortening it lets tests exercise hang/timeout faults quickly.
	ConnectorTimeout time.Duration
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
	if o.ConnectorTimeout > 0 {
		w.opts.ConnectorTimeout = o.ConnectorTimeout
	}
	return w
}

// adapterFor builds a provider adapter honoring the configured connector
// timeout (or the registry default when unset).
func (w *Worker) adapterFor(provider, baseURL string) (connectors.Adapter, error) {
	return registry.NewWithTimeout(provider, baseURL, "", w.opts.ConnectorTimeout, 0)
}

// recordSyncOperation appends one row to the sync_operations ledger for an
// applied destination write. Best-effort: a ledger failure must not fail the
// propagation itself.
func (w *Worker) recordSyncOperation(ctx context.Context, tenant, entityType, entityID, source, target string, version int64, eventID, fingerprint string) {
	if _, err := store.InsertSyncOperation(ctx, w.db.App, store.SyncOperation{
		TenantID:       tenant,
		EntityType:     entityType,
		EntityID:       entityID,
		Source:         source,
		TargetSource:   target,
		EventID:        eventID,
		AppliedVersion: version,
		Fingerprint:    fingerprint,
	}); err != nil {
		w.log.Warn("sync operation ledger write failed", "entity", entityID, "target", target, "error", err)
	}
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
	ctx, span := otel.Tracer("syncworker").Start(ctx, "worker.process",
		trace.WithAttributes(
			attribute.String("event_id", ev.EventID),
			attribute.String("source", ev.Source),
			attribute.String("entity_type", ev.EntityType),
			attribute.String("entity_id", ev.EntityID),
			attribute.String("event_type", string(ev.EventType)),
			attribute.String("tenant_id", ev.TenantID),
		))
	defer span.End()

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

	// A reconcile repair event applies a parked/auto finding directly: the
	// engine (or operator) has already decided the direction, so no ordering,
	// echo, or conflict checks re-run.
	if ev.Provenance.ReconcileFindingID != "" {
		if err := w.applyReconcileFinding(ctx, ev, entityType, policies); err != nil {
			w.fail(ctx, ev, err)
			return err
		}
		if err := store.SetSourceEventStatus(ctx, w.db.App, ev.TenantID, ev.EventID, "validated", "processed"); err != nil {
			w.log.Warn("mark reconcile finding event processed", "event_id", ev.EventID, "error", err)
		}
		w.metrics.EventsSuccess.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(ev.Source)))
		w.metrics.ProcessingDuration.Record(ctx, time.Since(start).Seconds())
		w.log.Info("reconcile finding applied",
			"event_id", ev.EventID, "source", ev.Source, "finding", ev.Provenance.ReconcileFindingID)
		return nil
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
	trace.SpanFromContext(ctx).SetStatus(codes.Ok, "applied")
	w.log.Info("event processed",
		"event_id", ev.EventID, "source", ev.Source, "entity", ev.EntityID, "type", ev.EventType)
	return nil
}

// fail marks an event as failed and releases its idempotency claim so the
// durable retry machinery (Phase 4) can re-run it.
func (w *Worker) fail(ctx context.Context, ev *events.Event, cause error) {
	span := trace.SpanFromContext(ctx)
	span.RecordError(cause)
	span.SetStatus(codes.Error, cause.Error())
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
	srcAdapter, err := w.adapterFor(srcConn.Provider, srcConn.BaseURL)
	if err != nil {
		return "", err
	}
	return srcAdapter.CanonicalEntityType(), nil
}

// applyUpsert handles created/updated events: identity resolution, version
// check, loop-prevention, conflict detection + resolution, then propagation to
// every configured destination.
func (w *Worker) applyUpsert(ctx context.Context, ev *events.Event, entityType string, policies []store.SyncPolicy) error {
	tenant := ev.TenantID
	fields, ok := ev.Payload["fields"].(map[string]any)
	if !ok || len(fields) == 0 {
		return connectors.NewError(connectors.ErrSchema, "event payload missing fields", nil)
	}

	// An operator-forced resolution applies the chosen side directly: no
	// ordering, echo, conflict, or provider-normalization checks — the
	// operator has decided the state.
	if ev.Provenance.ResolvedConflictID != "" {
		return w.applyResolution(ctx, ev, entityType, policies, fields)
	}

	srcConn, err := store.GetConnectionByProvider(ctx, w.db.App, tenant, ev.Source)
	if err != nil {
		return err
	}
	srcAdapter, err := w.adapterFor(srcConn.Provider, srcConn.BaseURL)
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

	// 4. Conflict detection + resolution: an incoming event that changes a
	//    field another source last wrote is a concurrent edit. The active
	//    policy strategy decides whether we auto-resolve (and keep an audit
	//    row) or park it for a human.
	strategy := conflict.StrategyLastWriteWins
	priority := 100
	if len(policies) > 0 {
		if p := policies[0].ConflictStrategy; p != "" {
			strategy = p
		}
		priority = policies[0].SourcePriority
	}

	merged, mergedFP, detected, manual := conflict.Merge(
		strategy,
		canonical.Fields,
		conflict.FromMap(canonical.FieldProvenance),
		cust.Fields(),
		ev.Source, ev.SourceVersion, ev.OccurredAt,
		priority,
	)
	if manual {
		// Manual strategy: do not apply. Park a CONFLICT_PENDING for the
		// operator to resolve; the event is considered handled (no retry).
		w.metrics.ConflictsDetected.Add(ctx, 1)
		w.recordConflict(ctx, ev, entityType, canonical, cust.Fields(), detected, store.ConflictPending, strategy)
		w.log.Warn("conflict parked for manual resolution",
			"event_id", ev.EventID, "entity", canonical.EntityID,
			"fields", len(detected), "strategy", strategy)
		return nil
	}
	if len(detected) > 0 {
		w.metrics.ConflictsDetected.Add(ctx, 1)
		w.metrics.ConflictsResolved.Add(ctx, 1)
		w.recordConflict(ctx, ev, entityType, canonical, cust.Fields(), detected, store.ConflictAutoResolved, strategy)
		w.log.Warn("conflict auto-resolved",
			"event_id", ev.EventID, "entity", canonical.EntityID,
			"fields", len(detected), "strategy", strategy)
	}
	// Propagate the resolved (or plain) field set, and persist its ownership.
	cust.FromFields(merged)
	canonical.FieldProvenance = mergedFP.ToMap()

	// 5. Propagate to every destination.
	var outboundWrites []store.OutboundWrite
	var syncOps []store.SyncOperation
	for _, policy := range policies {
		dstConn, err := store.GetConnectionByProvider(ctx, w.db.App, tenant, policy.Destination)
		if err != nil {
			return err
		}
		dstAdapter, err := w.adapterFor(dstConn.Provider, dstConn.BaseURL)
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
		outboundWrites = append(outboundWrites, store.OutboundWrite{
			TenantID:       tenant,
			EntityType:     entityType,
			EntityID:       canonical.EntityID,
			TargetSource:   policy.Destination,
			Fingerprint:    cust.Fingerprint(),
			AppliedVersion: ev.SourceVersion,
		})
		syncOps = append(syncOps, store.SyncOperation{
			TenantID:       tenant,
			EntityType:     entityType,
			EntityID:       canonical.EntityID,
			Source:         ev.Source,
			TargetSource:   policy.Destination,
			EventID:        ev.EventID,
			AppliedVersion: ev.SourceVersion,
			Fingerprint:    cust.Fingerprint(),
		})
	}

	// 6. Persist canonical state + outbound fingerprints + ledger in one
	// transaction (single round trip; PostgreSQL is the apply-path bottleneck).
	canonical.Fields = cust.Fields()
	canonical.SourceVersions[ev.Source] = ev.SourceVersion
	canonical.Version++
	canonical.OriginSource = ev.Source
	canonical.OriginEventID = ev.EventID
	canonical.Tombstone = false
	_, err = store.PersistApplyState(ctx, w.db.App, canonical, outboundWrites, syncOps)
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
		dstAdapter, err := w.adapterFor(dstConn.Provider, dstConn.BaseURL)
		if err != nil {
			return err
		}
		if err := dstAdapter.Delete(ctx, dstID); err != nil {
			return err
		}
		w.metrics.DestinationWrites.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(policy.Destination)))
		w.recordSyncOperation(ctx, tenant, entityType, canonical.EntityID, ev.Source,
			policy.Destination, ev.SourceVersion, ev.EventID, "")
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

// recordConflict durably persists a detected conflict (either parked for a
// manual resolution or recorded as an audit trail for auto-resolutions). Both
// sides' canonical field snapshots are stored so an operator can inspect and
// pick a winner. Idempotent per the (tenant, entity, source pair, versions)
// key.
func (w *Worker) recordConflict(ctx context.Context, ev *events.Event, entityType string, canonical store.CanonicalRecord, incomingFields map[string]any, detected []conflict.FieldConflict, status, strategy string) error {
	if len(detected) == 0 {
		return nil
	}
	c := detected[0]
	payloadA, err := json.Marshal(canonical.Fields)
	if err != nil {
		return err
	}
	payloadB, err := json.Marshal(incomingFields)
	if err != nil {
		return err
	}
	_, err = store.InsertConflict(ctx, w.db.App, store.ConflictRecord{
		TenantID:           ev.TenantID,
		EntityType:         entityType,
		EntityID:           canonical.EntityID,
		SourceA:            c.SourceA,
		VersionA:           c.VersionA,
		PayloadA:           payloadA,
		SourceB:            c.SourceB,
		VersionB:           c.VersionB,
		PayloadB:           payloadB,
		Status:             status,
		ResolutionStrategy: strategy,
	})
	if err != nil {
		w.log.Warn("record conflict", "event_id", ev.EventID, "entity", canonical.EntityID, "error", err)
	}
	return err
}

// applyResolution applies an operator-chosen conflict resolution: the chosen
// side's canonical fields are written to every destination, the canonical
// record and its provenance are updated, and the conflict is marked resolved.
// The payload is already canonical fields (not provider-native), so no source
// adapter normalization occurs.
func (w *Worker) applyResolution(ctx context.Context, ev *events.Event, entityType string, policies []store.SyncPolicy, fields map[string]any) error {
	tenant := ev.TenantID
	canonical, err := store.GetCanonical(ctx, w.db.App, tenant, entityType, ev.EntityID)
	if errors.Is(err, store.ErrNotFound) {
		w.log.Warn("resolution ignored: canonical record gone", "entity", ev.EntityID, "conflict", ev.Provenance.ResolvedConflictID)
		return nil
	}
	if err != nil {
		return err
	}

	cust := &model.Customer{TenantID: tenant, EntityID: canonical.EntityID}
	cust.FromFields(fields)

	// Write the winning state to every configured destination.
	for _, policy := range policies {
		dstConn, err := store.GetConnectionByProvider(ctx, w.db.App, tenant, policy.Destination)
		if err != nil {
			return err
		}
		dstAdapter, err := w.adapterFor(dstConn.Provider, dstConn.BaseURL)
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
		} else if _, err := dstAdapter.Update(ctx, dstID, dstRec); err != nil {
			return err
		}
		w.metrics.DestinationWrites.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(policy.Destination)))
		if err := store.UpsertOutboundWrite(ctx, w.db.App, store.OutboundWrite{
			TenantID:     tenant,
			EntityType:   entityType,
			EntityID:     canonical.EntityID,
			TargetSource: policy.Destination,
			Fingerprint:  cust.Fingerprint(),
		}); err != nil {
			return err
		}
		w.recordSyncOperation(ctx, tenant, entityType, canonical.EntityID, ev.Source,
			policy.Destination, ev.SourceVersion, ev.EventID, cust.Fingerprint())
	}

	// The winning source now owns every field it carried forward; other fields
	// keep their existing writer.
	fp := conflict.FromMap(canonical.FieldProvenance)
	for f := range fields {
		prio := 100
		if existing, ok := fp[f]; ok {
			prio = existing.Priority
		}
		fp[f] = conflict.Provenance{Source: ev.Source, Version: ev.SourceVersion, OccurredAt: ev.OccurredAt, Priority: prio}
	}

	canonical.Fields = cust.Fields()
	canonical.FieldProvenance = fp.ToMap()
	canonical.SourceVersions[ev.Source] = ev.SourceVersion
	canonical.Version++
	canonical.OriginSource = ev.Source
	canonical.OriginEventID = ev.EventID
	canonical.Tombstone = false
	if _, err := store.UpsertCanonical(ctx, w.db.App, canonical); err != nil {
		return err
	}

	if err := store.SetConflictStatus(ctx, w.db.App, tenant, ev.Provenance.ResolvedConflictID, store.ConflictResolved, "manual", "operator"); err != nil {
		w.log.Warn("mark conflict resolved", "conflict", ev.Provenance.ResolvedConflictID, "error", err)
	}
	w.metrics.ConflictsResolved.Add(ctx, 1)
	w.log.Info("conflict resolved by operator",
		"conflict", ev.Provenance.ResolvedConflictID, "entity", canonical.EntityID,
		"winner", ev.Source, "fields", len(fields))
	return nil
}

// applyReconcileFinding applies an operator-approved or auto-mode repair for a
// reconciliation finding. The direction was decided by the engine (or the
// operator approving a manual finding), so no conflict, ordering, or echo
// checks re-run. Success transitions the finding to applied; a retryable
// failure marks it failed (the retry machinery may re-run the same event id).
func (w *Worker) applyReconcileFinding(ctx context.Context, ev *events.Event, entityType string, policies []store.SyncPolicy) error {
	tenant := ev.TenantID
	finding, err := store.GetReconciliationFinding(ctx, w.db.App, tenant, ev.Provenance.ReconcileFindingID)
	if errors.Is(err, store.ErrNotFound) {
		w.log.Warn("reconcile repair skipped: finding gone", "finding", ev.Provenance.ReconcileFindingID)
		return nil
	}
	if err != nil {
		return err
	}
	// Terminal states (applied/dismissed/skipped/failed) are never re-applied.
	if finding.Status != store.FindingPending {
		return nil
	}

	run, err := store.GetReconciliationRun(ctx, w.db.App, tenant, finding.RunID)
	if err != nil {
		return err
	}
	srcConn, err := store.GetConnectionByProvider(ctx, w.db.App, tenant, run.Source)
	if err != nil {
		return err
	}
	srcAdapter, err := w.adapterFor(srcConn.Provider, srcConn.BaseURL)
	if err != nil {
		return err
	}

	switch finding.Direction {
	case "delete":
		// The canonical record is tombstoned but the provider still serves a
		// live record: push the delete. NOT_FOUND means the provider already
		// cleaned it up.
		err := srcAdapter.Delete(ctx, finding.ProviderID)
		if err != nil {
			if !connectors.IsKind(err, connectors.ErrNotFound) {
				return w.reconcileFail(ctx, finding, err)
			}
		}
		w.metrics.DestinationWrites.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(run.Source)))
		w.recordSyncOperation(ctx, tenant, entityType, finding.CanonicalID, run.Source,
			run.Source, finding.ProviderVersion, ev.EventID, "")
		return w.reconcileApplied(ctx, finding)

	case "push_canonical":
		// Canonical wins: write canonical state back to the provider. The
		// record is updated when the provider already has it (drift); a
		// missing record is re-created and the new id mapped.
		canonical, err := store.GetCanonical(ctx, w.db.App, tenant, entityType, finding.CanonicalID)
		if errors.Is(err, store.ErrNotFound) {
			return w.reconcileSkip(ctx, finding, "canonical record gone")
		}
		if err != nil {
			return err
		}
		cust := &model.Customer{TenantID: tenant, EntityID: canonical.EntityID}
		cust.FromFields(canonical.Fields)
		dstRec, err := srcAdapter.Denormalize(cust)
		if err != nil {
			return w.reconcileFail(ctx, finding, err)
		}

		providerID := finding.ProviderID
		var updated connectors.ProviderRecord
		if finding.Kind == store.FindingMissing || providerID == "" {
			created, err := srcAdapter.Create(ctx, dstRec)
			if err != nil {
				return w.reconcileFail(ctx, finding, err)
			}
			providerID, updated = created.ID, created
		} else {
			updated, err = srcAdapter.Update(ctx, providerID, dstRec)
			if err != nil {
				if !connectors.IsKind(err, connectors.ErrNotFound) {
					return w.reconcileFail(ctx, finding, err)
				}
				// Provider deleted it concurrently: recreate to converge.
				created, err := srcAdapter.Create(ctx, dstRec)
				if err != nil {
					return w.reconcileFail(ctx, finding, err)
				}
				providerID, updated = created.ID, created
			}
		}

		// Persist the mapping + source version so future events converge, and
		// record the outbound fingerprint so the provider's echo of our write
		// is recognized and dropped.
		canonical.ProviderIDs[run.Source] = providerID
		canonical.SourceVersions[run.Source] = updated.SourceVersion
		canonical.Version++
		canonical.Tombstone = false
		if _, err := store.UpsertCanonical(ctx, w.db.App, canonical); err != nil {
			return err
		}
		if err := store.UpsertOutboundWrite(ctx, w.db.App, store.OutboundWrite{
			TenantID:       tenant,
			EntityType:     entityType,
			EntityID:       canonical.EntityID,
			TargetSource:   run.Source,
			Fingerprint:    cust.Fingerprint(),
			AppliedVersion: updated.SourceVersion,
		}); err != nil {
			return err
		}
		w.metrics.DestinationWrites.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(run.Source)))
		w.recordSyncOperation(ctx, tenant, entityType, canonical.EntityID, run.Source,
			run.Source, finding.ProviderVersion, ev.EventID, cust.Fingerprint())
		return w.reconcileApplied(ctx, finding)

	case "adopt_provider":
		// Provider wins: the finding's provider fields become canonical and are
		// propagated to every destination. Used for missed records (provider
		// has a record we never ingested) and drift repairs where the operator
		// picked the provider side.
		return w.applyAdoptProvider(ctx, ev, entityType, policies, finding, run, srcAdapter)

	default:
		return w.reconcileFail(ctx, finding, fmt.Errorf("unknown reconcile direction %q", finding.Direction))
	}
}

// applyAdoptProvider treats the provider's state as authoritative: resolve (or
// create) the canonical entity from the provider record and propagate the
// provider fields to every configured destination.
func (w *Worker) applyAdoptProvider(ctx context.Context, ev *events.Event, entityType string, policies []store.SyncPolicy, finding store.ReconciliationFinding, run store.ReconciliationRun, srcAdapter connectors.Adapter) error {
	tenant := ev.TenantID
	cust := &model.Customer{TenantID: tenant, EntityID: finding.ProviderID}
	cust.FromFields(finding.ProviderFields)

	canonical, err := store.GetCanonicalByProvider(ctx, w.db.App, tenant, entityType, run.Source, finding.ProviderID)
	if errors.Is(err, store.ErrNotFound) {
		// Identity resolution by email (mirrors the ingest path) before
		// creating a fresh canonical record.
		canonical, err = store.GetCanonicalByEmail(ctx, w.db.App, tenant, entityType, cust.Email)
		if errors.Is(err, store.ErrNotFound) {
			canonical = store.CanonicalRecord{
				TenantID:        tenant,
				EntityType:      entityType,
				EntityID:        finding.ProviderID,
				ProviderIDs:     map[string]string{run.Source: finding.ProviderID},
				SourceVersions:  map[string]int64{},
				FieldProvenance: map[string]any{},
			}
		} else if err != nil {
			return err
		} else {
			if err := store.AddProviderID(ctx, w.db.App, tenant, entityType, canonical.EntityID, run.Source, finding.ProviderID); err != nil {
				return err
			}
			canonical.ProviderIDs[run.Source] = finding.ProviderID
		}
	} else if err != nil {
		return err
	}
	if canonical.EntityID == "" {
		canonical.EntityID = finding.ProviderID
	}
	cust.EntityID = canonical.EntityID

	// Propagate the provider state to every destination configured for this
	// source.
	for _, policy := range policies {
		if policy.Source != run.Source {
			continue
		}
		dstConn, err := store.GetConnectionByProvider(ctx, w.db.App, tenant, policy.Destination)
		if err != nil {
			return err
		}
		dstAdapter, err := w.adapterFor(dstConn.Provider, dstConn.BaseURL)
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
		} else if _, err := dstAdapter.Update(ctx, dstID, dstRec); err != nil {
			return err
		}
		w.metrics.DestinationWrites.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(policy.Destination)))
		if err := store.UpsertOutboundWrite(ctx, w.db.App, store.OutboundWrite{
			TenantID:     tenant,
			EntityType:   entityType,
			EntityID:     canonical.EntityID,
			TargetSource: policy.Destination,
			Fingerprint:  cust.Fingerprint(),
		}); err != nil {
			return err
		}
		w.recordSyncOperation(ctx, tenant, entityType, canonical.EntityID, run.Source,
			policy.Destination, finding.ProviderVersion, ev.EventID, cust.Fingerprint())
	}

	canonical.Fields = cust.Fields()
	canonical.FieldProvenance = map[string]any{}
	for k := range cust.Fields() {
		canonical.FieldProvenance[k] = map[string]any{
			"source":      run.Source,
			"version":     finding.ProviderVersion,
			"occurred_at": ev.OccurredAt,
			"priority":    100,
		}
	}
	canonical.SourceVersions[run.Source] = finding.ProviderVersion
	canonical.Version++
	canonical.OriginSource = run.Source
	canonical.OriginEventID = ev.EventID
	canonical.Tombstone = false
	if _, err := store.UpsertCanonical(ctx, w.db.App, canonical); err != nil {
		return err
	}
	return w.reconcileApplied(ctx, finding)
}

// reconcileApplied transitions a finding to applied and records a repair.
func (w *Worker) reconcileApplied(ctx context.Context, finding store.ReconciliationFinding) error {
	if err := store.SetReconciliationFindingStatus(ctx, w.db.App, finding.TenantID, finding.ID, store.FindingApplied, nil); err != nil {
		w.log.Warn("mark reconcile finding applied", "finding", finding.ID, "error", err)
	}
	w.metrics.ReconcileRepairs.Add(ctx, 1)
	return nil
}

// reconcileSkip parks a finding that cannot be applied (e.g. the canonical
// record is gone). Skipped is terminal.
func (w *Worker) reconcileSkip(ctx context.Context, finding store.ReconciliationFinding, reason string) error {
	if err := store.SetReconciliationFindingStatus(ctx, w.db.App, finding.TenantID, finding.ID, store.FindingSkipped, &reason); err != nil {
		w.log.Warn("mark reconcile finding skipped", "finding", finding.ID, "error", err)
	}
	return nil
}

// reconcileFail marks a finding failed with the cause. It returns the error so
// the caller's failure path (retry/DLQ) still owns the event.
func (w *Worker) reconcileFail(ctx context.Context, finding store.ReconciliationFinding, cause error) error {
	msg := cause.Error()
	if err := store.SetReconciliationFindingStatus(ctx, w.db.App, finding.TenantID, finding.ID, store.FindingFailed, &msg); err != nil {
		w.log.Warn("mark reconcile finding failed", "finding", finding.ID, "error", err)
	}
	return cause
}
