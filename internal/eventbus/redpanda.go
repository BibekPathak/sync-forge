package eventbus

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RedpandaBus is a Kafka-protocol transport backed by Redpanda. Producer and
// consumer clients are separate; consumers use manual offset commits so a
// message is only committed after its handler returns nil (at-least-once).
type RedpandaBus struct {
	producer *kgo.Client
	brokers  []string
	log      *slog.Logger
}

func NewRedpanda(brokers []string, log *slog.Logger) (*RedpandaBus, error) {
	if log == nil {
		log = slog.Default()
	}
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
	)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}
	return &RedpandaBus{producer: producer, brokers: brokers, log: log}, nil
}

func (b *RedpandaBus) Publish(ctx context.Context, topic string, key, value []byte) error {
	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	if err := b.producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	return nil
}

func (b *RedpandaBus) Subscribe(ctx context.Context, topic, group string, handler Handler) error {
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(b.brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer consumer.Close()

	b.log.Info("subscribing", "topic", topic, "group", group)

	for {
		fetches := consumer.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				b.log.Error("fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
			}
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if err := handler(ctx, string(rec.Key), rec.Value); err != nil {
				b.log.Warn("message handler failed (not committed)",
					"key", string(rec.Key), "topic", rec.Topic, "partition", rec.Partition,
					"offset", rec.Offset, "error", err)
				continue
			}
			consumer.MarkCommitRecords(rec)
		}

		// Non-blocking commit of any marked offsets.
		if err := consumer.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			b.log.Warn("offset commit failed", "error", err)
		}

		// Pause briefly when the topic is empty to avoid a busy loop.
		if fetches.Empty() {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

func (b *RedpandaBus) Health(ctx context.Context) error {
	return b.producer.Ping(ctx)
}

func (b *RedpandaBus) Close() error {
	b.producer.Close()
	return nil
}
