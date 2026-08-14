package eventbus

import "context"

// Topic is the canonical sync-event stream. Partition keys are
// tenant:entity_type:entity_id, giving per-entity ordering.
const TopicSyncEvents = "sync.events"

// Handler processes one message. Returning an error means the message must not
// be treated as processed (drives at-least-once redelivery where the transport
// supports it).
type Handler func(ctx context.Context, key string, value []byte) error

// Bus is the durable message transport. Production uses Redpanda/Kafka;
// tests use the in-memory implementation.
type Bus interface {
	// Publish writes a message to a topic. The key routes the message and
	// guarantees ordering per key.
	Publish(ctx context.Context, topic string, key []byte, value []byte) error
	// Subscribe consumes a topic as a consumer group. handler must return nil
	// to mark a message processed. Blocks until ctx is cancelled.
	Subscribe(ctx context.Context, topic, group string, handler Handler) error
	// Health reports transport liveness.
	Health(ctx context.Context) error
	Close() error
}
