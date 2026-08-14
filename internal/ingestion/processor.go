package ingestion

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"syncforge/internal/db"
	"syncforge/internal/eventbus"
	"syncforge/internal/events"
	"syncforge/internal/store"
)

// Processor drains not-yet-published webhook events from source_events and
// publishes them to the durable topic. The gateway is broker-independent:
// events sit durably in PostgreSQL until this processor picks them up.
type Processor struct {
	db        *db.DB
	bus       eventbus.Bus
	log       *slog.Logger
	pollEvery time.Duration
	batchSize int
}

func New(database *db.DB, bus eventbus.Bus, log *slog.Logger) *Processor {
	if log == nil {
		log = slog.Default()
	}
	return &Processor{
		db:        database,
		bus:       bus,
		log:       log,
		pollEvery: 200 * time.Millisecond,
		batchSize: 100,
	}
}

// Run polls source_events until ctx is cancelled.
func (p *Processor) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.pollEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.drain(ctx)
		}
	}
}

func (p *Processor) drain(ctx context.Context) {
	pending, err := store.PendingSourceEvents(ctx, p.db.Admin, p.batchSize)
	if err != nil {
		p.log.Error("scan pending events", "error", err)
		return
	}
	for _, se := range pending {
		ev := fromSourceEvent(se)
		payload, err := json.Marshal(ev)
		if err != nil {
			p.log.Error("marshal sync event", "event_id", se.EventID, "error", err)
			continue
		}
		key := []byte(ev.PartitionKey())
		if err := p.bus.Publish(ctx, eventbus.TopicSyncEvents, key, payload); err != nil {
			p.log.Error("publish event", "event_id", se.EventID, "error", err)
			// leave status='received' so the next poll retries
			continue
		}
		if err := store.SetSourceEventStatus(ctx, p.db.App, se.TenantID, se.EventID, "received", "validated"); err != nil {
			p.log.Error("mark event validated", "event_id", se.EventID, "error", err)
		}
		p.log.Debug("published sync event", "event_id", se.EventID, "key", string(key))
	}
}

// fromSourceEvent rebuilds the canonical sync event from a stored raw webhook.
func fromSourceEvent(se store.SourceEvent) events.Event {
	var payload map[string]any
	if wh, ok := se.Raw["webhook"].(map[string]any); ok {
		if p, ok := wh["payload"].(map[string]any); ok {
			payload = p
		}
	}
	prov := events.Provenance{}
	if s, ok := se.Provenance["origin_source"].(string); ok {
		prov.OriginSource = s
	}
	if s, ok := se.Provenance["origin_event_id"].(string); ok {
		prov.OriginEventID = s
	}
	if s, ok := se.Provenance["sync_operation_id"].(string); ok {
		prov.SyncOperationID = s
	}

	ev := events.Event{
		EventID:       se.EventID,
		TenantID:      se.TenantID,
		Source:        se.Source,
		EntityType:    se.EntityType,
		EntityID:      se.EntityID,
		EventType:     events.EventType(se.EventType),
		SourceVersion: se.SourceVersion,
		ReceivedAt:    time.Now().UTC(),
		CorrelationID: se.CorrelationID,
		Provenance:    prov,
		Payload:       payload,
	}
	if se.OccurredAt != nil {
		ev.OccurredAt = *se.OccurredAt
	}
	return ev
}
