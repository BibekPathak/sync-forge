package simulator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// WebhookDispatcher pushes provider-native change notifications to a target
// (SyncForge's webhook gateway), signed with an HMAC so the receiver can
// authenticate them.
type WebhookDispatcher struct {
	URL        string
	Secret     string
	Source     string
	EntityType string
	IDKey      string
	TimeKey    string
	Client     *http.Client
	Faults     *FaultManager
	Log        *slog.Logger
}

type webhookEvent struct {
	EventID       string         `json:"event_id"`
	Source        string         `json:"source"`
	EntityType    string         `json:"entity_type"`
	EntityID      string         `json:"entity_id"`
	EventType     string         `json:"event_type"`
	SourceVersion int64          `json:"source_version"`
	OccurredAt    time.Time      `json:"occurred_at"`
	Payload       map[string]any `json:"payload"`
}

// Emit sends a webhook asynchronously for a mutation. It applies fault
// injection (drop, duplication, delay, out-of-order jitter).
func (w *WebhookDispatcher) Emit(ctx context.Context, eventType string, rec *Record) {
	if w == nil || w.URL == "" {
		return
	}
	drop, _, outOfOrder, copies, delay, extra := w.Faults.WebhookOptions()
	if drop {
		w.Log.Debug("webhook dropped (fault)", "entity_id", rec.ID)
		return
	}

	payload := cloneMap(rec.Data)
	if rec.ID != "" {
		payload[w.IDKey] = rec.ID
	}

	ev := webhookEvent{
		EventID:       uuid.NewString(),
		Source:        w.Source,
		EntityType:    w.EntityType,
		EntityID:      rec.ID,
		EventType:     eventType,
		SourceVersion: rec.Version,
		OccurredAt:    rec.UpdatedAt,
		Payload:       map[string]any{"fields": payload},
	}

	baseDelay := delay
	for i := 0; i < copies; i++ {
		d := baseDelay
		if outOfOrder && extra > 0 {
			d += time.Duration(w.Faults.rng.Intn(int(extra.Milliseconds()))) * time.Millisecond
		}
		go w.deliver(ctx, ev, d)
	}
}

func (w *WebhookDispatcher) deliver(ctx context.Context, ev webhookEvent, delay time.Duration) {
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
	}
	body, err := json.Marshal(ev)
	if err != nil {
		w.Log.Error("webhook marshal failed", "error", err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	mac := hmac.New(sha256.New, []byte(w.Secret))
	mac.Write(body)
	req.Header.Set("X-SyncForge-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))

	resp, err := w.Client.Do(req)
	if err != nil {
		w.Log.Error("webhook delivery failed", "event_id", ev.EventID, "error", err)
		return
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		w.Log.Error("webhook rejected", "event_id", ev.EventID, "status", resp.StatusCode)
	}
}
