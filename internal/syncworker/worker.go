package syncworker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"

	"syncforge/internal/connectors"
	"syncforge/internal/connectors/registry"
	"syncforge/internal/db"
	"syncforge/internal/events"
	"syncforge/internal/observability"
	"syncforge/internal/store"
)

// Worker consumes canonical sync events, applies them to destination systems,
// and persists canonical state. Idempotency is enforced via processed_events.
type Worker struct {
	db      *db.DB
	log     *slog.Logger
	metrics *observability.SyncMetrics
}

func New(database *db.DB, metrics *observability.SyncMetrics, log *slog.Logger) *Worker {
	if log == nil {
		log = slog.Default()
	}
	return &Worker{db: database, metrics: metrics, log: log}
}

// Handle implements eventbus.Handler.
func (w *Worker) Handle(ctx context.Context, _ string, value []byte) error {
	var ev events.Event
	if err := json.Unmarshal(value, &ev); err != nil {
		return err
	}
	return w.Process(ctx, &ev)
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
	var targets []store.SyncPolicy
	for _, p := range policies {
		if p.Source == ev.Source && p.Entity == ev.EntityType {
			targets = append(targets, p)
		}
	}
	if len(targets) == 0 {
		w.log.Info("no policy for event, releasing claim", "event_id", ev.EventID, "source", ev.Source)
		_ = store.ReleaseProcessedEvent(ctx, w.db.App, ev.TenantID, ev.Source, ev.EventID)
		_ = store.SetSourceEventStatus(ctx, w.db.App, ev.TenantID, ev.EventID, "validated", "processed")
		return nil
	}

	for _, policy := range targets {
		if err := w.apply(ctx, ev, policy); err != nil {
			w.fail(ctx, ev, err)
			return err
		}
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

func (w *Worker) apply(ctx context.Context, ev *events.Event, policy store.SyncPolicy) error {
	srcConn, err := store.GetConnectionByProvider(ctx, w.db.App, ev.TenantID, ev.Source)
	if err != nil {
		return err
	}
	dstConn, err := store.GetConnectionByProvider(ctx, w.db.App, ev.TenantID, policy.Destination)
	if err != nil {
		return err
	}
	srcAdapter, err := registry.New(srcConn.Provider, srcConn.BaseURL, "")
	if err != nil {
		return err
	}
	dstAdapter, err := registry.New(dstConn.Provider, dstConn.BaseURL, "")
	if err != nil {
		return err
	}

	if ev.EventType == events.EventDeleted {
		return w.applyDelete(ctx, ev, policy, dstAdapter)
	}
	return w.applyUpsert(ctx, ev, policy, srcAdapter, dstAdapter)
}

func (w *Worker) applyUpsert(ctx context.Context, ev *events.Event, policy store.SyncPolicy, srcAdapter connectors.Adapter, dstAdapter connectors.Adapter) error {
	fields, ok := ev.Payload["fields"].(map[string]any)
	if !ok || len(fields) == 0 {
		return connectors.NewError(connectors.ErrSchema, "event payload missing fields", nil)
	}
	srcRec := connectors.ProviderRecord{ID: ev.EntityID, SourceVersion: ev.SourceVersion, Data: fields}
	if err := srcAdapter.Validate(srcRec); err != nil {
		return err
	}
	cust, err := srcAdapter.Normalize(srcRec)
	if err != nil {
		return err
	}
	cust.TenantID = ev.TenantID

	canonical, err := store.GetCanonicalByProvider(ctx, w.db.App, ev.TenantID, ev.EntityType, ev.Source, ev.EntityID)
	if errors.Is(err, store.ErrNotFound) {
		canonical = store.CanonicalRecord{
			TenantID:       ev.TenantID,
			EntityType:     ev.EntityType,
			EntityID:       ev.EntityID,
			Version:        0,
			ProviderIDs:    map[string]string{ev.Source: ev.EntityID},
			SourceVersions: map[string]int64{},
		}
	} else if err != nil {
		return err
	}

	// Phase 3 inserts version/ordering checks and conflict detection here.
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
		w.metrics.DestinationWrites.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(policy.Destination)))
	} else {
		if _, err := dstAdapter.Update(ctx, dstID, dstRec); err != nil {
			return err
		}
		w.metrics.DestinationWrites.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(policy.Destination)))
	}

	canonical.Fields = cust.Fields()
	canonical.SourceVersions[ev.Source] = ev.SourceVersion
	canonical.Version++
	canonical.OriginSource = ev.Source
	canonical.OriginEventID = ev.EventID
	canonical.Tombstone = false
	if canonical.FieldProvenance == nil {
		canonical.FieldProvenance = map[string]any{}
	}

	_, err = store.UpsertCanonical(ctx, w.db.App, canonical)
	return err
}

func (w *Worker) applyDelete(ctx context.Context, ev *events.Event, policy store.SyncPolicy, dstAdapter connectors.Adapter) error {
	canonical, err := store.GetCanonicalByProvider(ctx, w.db.App, ev.TenantID, ev.EntityType, ev.Source, ev.EntityID)
	if errors.Is(err, store.ErrNotFound) {
		// Nothing we know about to delete.
		return nil
	}
	if err != nil {
		return err
	}

	if policy.DeletePolicy == "propagate" {
		if dstID := canonical.ProviderIDs[policy.Destination]; dstID != "" {
			if err := dstAdapter.Delete(ctx, dstID); err != nil {
				return err
			}
			w.metrics.DestinationWrites.Add(ctx, 1, metric.WithAttributes(observability.SrcAttr(policy.Destination)))
		}
	}

	canonical.Tombstone = true
	canonical.SourceVersions[ev.Source] = ev.SourceVersion
	canonical.Version++
	canonical.OriginSource = ev.Source
	canonical.OriginEventID = ev.EventID
	_, err = store.UpsertCanonical(ctx, w.db.App, canonical)
	return err
}
