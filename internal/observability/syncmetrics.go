package observability

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SyncMetrics are the engine's synchronization counters.
type SyncMetrics struct {
	EventsTotal        metric.Int64Counter
	EventsSuccess      metric.Int64Counter
	EventsFailed       metric.Int64Counter
	Duplicates         metric.Int64Counter
	DestinationWrites  metric.Int64Counter
	ProcessingDuration metric.Float64Histogram
}

func NewSyncMetrics(meter metric.Meter) (*SyncMetrics, error) {
	eventsTotal, err := meter.Int64Counter("sync_events_total",
		metric.WithDescription("Synchronization events processed"))
	if err != nil {
		return nil, err
	}
	eventsSuccess, err := meter.Int64Counter("sync_events_success_total",
		metric.WithDescription("Synchronization events applied successfully"))
	if err != nil {
		return nil, err
	}
	eventsFailed, err := meter.Int64Counter("sync_events_failed_total",
		metric.WithDescription("Synchronization events that failed"))
	if err != nil {
		return nil, err
	}
	duplicates, err := meter.Int64Counter("sync_duplicates_total",
		metric.WithDescription("Duplicate events deduplicated by the idempotency log"))
	if err != nil {
		return nil, err
	}
	destWrites, err := meter.Int64Counter("sync_destination_writes_total",
		metric.WithDescription("Writes issued to destination providers"))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("sync_processing_duration_seconds",
		metric.WithDescription("Time to apply a sync event end-to-end"))
	if err != nil {
		return nil, err
	}
	return &SyncMetrics{
		EventsTotal:        eventsTotal,
		EventsSuccess:      eventsSuccess,
		EventsFailed:       eventsFailed,
		Duplicates:         duplicates,
		DestinationWrites:  destWrites,
		ProcessingDuration: duration,
	}, nil
}

// SrcAttr is a convenience helper for source/destination attributes.
func SrcAttr(source string) attribute.KeyValue {
	return attribute.String("source", source)
}
