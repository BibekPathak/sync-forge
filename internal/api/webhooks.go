package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"syncforge/internal/store"
)

// providerWebhook is the payload shape the simulated providers emit. It is
// provider-native (payload.fields holds raw provider fields).
type providerWebhook struct {
	EventID       string         `json:"event_id"`
	Source        string         `json:"source"`
	EntityType    string         `json:"entity_type"`
	EntityID      string         `json:"entity_id"`
	EventType     string         `json:"event_type"`
	SourceVersion int64          `json:"source_version"`
	OccurredAt    time.Time      `json:"occurred_at"`
	CorrelationID string         `json:"correlation_id,omitempty"`
	Provenance    map[string]any `json:"provenance,omitempty"`
	Payload       map[string]any `json:"payload"`
}

// handleWebhook is the webhook gateway. Routes are /webhooks/{provider}/{tenant_slug}.
// It authenticates the delivery via HMAC signature, persists the raw event
// durably (idempotently), and acknowledges.
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	provider := r.PathValue("provider")
	slug := r.PathValue("tenant_slug")

	tenant, err := store.GetTenantBySlug(r.Context(), s.db.Admin, slug)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "unknown tenant")
		return
	}
	if err != nil {
		s.log.Error("webhook tenant lookup", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	conn, err := store.GetConnectionByProvider(r.Context(), s.db.App, tenant.ID, provider)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "no connection for provider")
		return
	}
	if err != nil {
		s.log.Error("webhook connection lookup", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read body")
		return
	}

	if !verifySignature(body, conn.WebhookSecret, r.Header.Get("X-SyncForge-Signature")) {
		s.log.Warn("webhook signature mismatch", "provider", provider, "tenant", slug)
		writeError(w, http.StatusUnauthorized, "invalid signature")
		return
	}

	var wh providerWebhook
	if err := json.Unmarshal(body, &wh); err != nil {
		writeError(w, http.StatusBadRequest, "malformed webhook payload")
		return
	}
	if wh.EventID == "" {
		wh.EventID = uuid.NewString()
	}
	if wh.Source == "" {
		wh.Source = provider
	}
	if wh.EntityType == "" || wh.EntityID == "" {
		writeError(w, http.StatusBadRequest, "missing entity_type/entity_id")
		return
	}
	if wh.Provenance == nil {
		wh.Provenance = map[string]any{}
	}

	ev, err := store.InsertSourceEvent(r.Context(), s.db.App, store.SourceEvent{
		TenantID:      tenant.ID,
		Source:        wh.Source,
		EventID:       wh.EventID,
		EntityType:    wh.EntityType,
		EntityID:      wh.EntityID,
		EventType:     wh.EventType,
		SourceVersion: wh.SourceVersion,
		OccurredAt:    &wh.OccurredAt,
		CorrelationID: wh.CorrelationID,
		Provenance:    wh.Provenance,
		Raw:           map[string]any{"webhook": unmarshalToMap(body)},
	})
	if errors.Is(err, store.ErrDuplicate) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "duplicate", "event_id": wh.EventID})
		return
	}
	if err != nil {
		s.log.Error("persist source event", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to persist event")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":   "received",
		"event_id": wh.EventID,
		"id":       ev.ID,
	})
}

// verifySignature checks `sha256=<hex>` against an HMAC-SHA256 of the body
// using the connection's webhook secret. Constant-time comparison.
func verifySignature(body []byte, secret, header string) bool {
	if secret == "" {
		return false
	}
	const prefix = "sha256="
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	want, err := hex.DecodeString(strings.TrimPrefix(header, prefix))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	got := mac.Sum(nil)
	return subtle.ConstantTimeCompare(got, want) == 1
}

func unmarshalToMap(b []byte) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}
