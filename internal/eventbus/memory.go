package eventbus

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type memoryMsg struct {
	key   string
	value []byte
}

// MemoryBus is an in-process, in-memory Bus for unit and integration tests.
// It preserves ordering per subscriber; it provides no durability (tests only).
type MemoryBus struct {
	mu     sync.Mutex
	topics map[string][]chan memoryMsg
	log    *slog.Logger
}

func NewMemoryBus(log *slog.Logger) *MemoryBus {
	return &MemoryBus{topics: make(map[string][]chan memoryMsg), log: log}
}

func (b *MemoryBus) Publish(_ context.Context, topic string, key, value []byte) error {
	b.mu.Lock()
	chans := append([]chan memoryMsg(nil), b.topics[topic]...)
	b.mu.Unlock()
	msg := memoryMsg{key: string(key), value: value}
	for _, ch := range chans {
		ch <- msg
	}
	return nil
}

func (b *MemoryBus) Subscribe(ctx context.Context, topic, group string, handler Handler) error {
	ch := make(chan memoryMsg, 1024)
	b.mu.Lock()
	b.topics[topic] = append(b.topics[topic], ch)
	b.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return nil
		case m := <-ch:
			if err := handler(ctx, m.key, m.value); err != nil {
				// At-least-once semantics don't apply to the memory bus; log and
				// drop so tests can still observe delivery counts.
				b.log.Warn("memory bus handler error", "key", m.key, "error", err)
			}
		}
	}
}

func (b *MemoryBus) Health(context.Context) error { return nil }

// WaitForSubscribers blocks until at least n subscribers are registered for a
// topic (tests only, to avoid publishing before a consumer is ready).
func (b *MemoryBus) WaitForSubscribers(ctx context.Context, topic string, n int) error {
	for {
		b.mu.Lock()
		count := len(b.topics[topic])
		b.mu.Unlock()
		if count >= n {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (b *MemoryBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, chans := range b.topics {
		for _, ch := range chans {
			close(ch)
		}
	}
	b.topics = map[string][]chan memoryMsg{}
	return nil
}
