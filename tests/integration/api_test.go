//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"

	"syncforge/internal/api"
	"syncforge/internal/cache"
	"syncforge/internal/config"
	"syncforge/internal/observability"
	"syncforge/internal/store"
)

func newAPIServer(t *testing.T) (*httptest.Server, *api.Server) {
	t.Helper()
	database := newDB(t)

	cfg := config.Load()
	cfg.SeedAcme = true
	cfg.SeedSFBaseURL = "http://sim-salesforce:9081"
	cfg.SeedHubBaseURL = "http://sim-hubspot:9082"
	cfg.SeedSFSSecret = "sfs-dev-secret"
	cfg.SeedHubSecret = "sfh-dev-secret"

	c := cache.New("localhost:6379")
	metrics, err := observability.NewServiceMetrics(otel.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	srv := api.New(cfg, database, c, metrics, logger)
	if err := srv.SeedDemoTenant(context.Background()); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ts := httptest.NewServer(srv.Router(promhttp.Handler()))
	t.Cleanup(ts.Close)
	return ts, srv
}

// postWebhook sends a webhook payload signed with secret to the gateway.
func postWebhook(t *testing.T, ts *httptest.Server, secret, payload string) (*http.Response, []byte) {
	t.Helper()
	body := []byte(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/webhooks/salesforce/acme", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-SyncForge-Signature", sig)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, respBody
}

func TestWebhookIngestAndDuplicate(t *testing.T) {
	ctx := context.Background()
	ts, srv := newAPIServer(t)

	payload := `{
		"event_id": "evt-001",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000001",
		"event_type": "updated",
		"source_version": 3,
		"occurred_at": "2024-01-01T00:00:00Z",
		"payload": {"fields": {"first_name": "Ada", "email": "ada@example.com"}}
	}`

	resp, _ := postWebhook(t, ts, "sfs-dev-secret", payload)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	tenantID := tenantIDFor(t, srv, "acme")
	ev, err := store.GetSourceEvent(ctx, srv.DB().App, tenantID, "evt-001")
	if err != nil {
		t.Fatalf("source event not stored: %v", err)
	}
	if ev.EventType != "updated" || ev.SourceVersion != 3 {
		t.Fatalf("stored event mismatch: %+v", ev)
	}

	// Duplicate delivery acknowledged without creating a second row.
	resp, _ = postWebhook(t, ts, "sfs-dev-secret", payload)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for duplicate, got %d", resp.StatusCode)
	}

	var n int
	err = srv.DB().Admin.QueryRow(ctx,
		`SELECT count(*) FROM source_events WHERE event_id='evt-001'`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 stored event, got %d", n)
	}
}

func TestWebhookRejectsBadSignature(t *testing.T) {
	ts, _ := newAPIServer(t)
	payload := `{"event_id":"evt-002","source":"salesforce","entity_type":"customer","entity_id":"x","event_type":"created","payload":{"fields":{}}}`
	resp, _ := postWebhook(t, ts, "wrong-secret", payload)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad signature, got %d", resp.StatusCode)
	}
}

func TestConnectionsAPIWithRLS(t *testing.T) {
	ts, _ := newAPIServer(t)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/connections", nil)
	req.Header.Set("X-API-Key", "sfk_acme_dev")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var body struct {
		Connections []store.Connection `json:"connections"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Connections) != 2 {
		t.Fatalf("expected 2 seeded connections, got %d", len(body.Connections))
	}

	// An invalid key is rejected.
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/api/v1/connections", nil)
	req.Header.Set("X-API-Key", "nope")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad api key, got %d", resp.StatusCode)
	}

	// Tenant management requires an ADMIN role API key (RBAC).
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/api/v1/tenants", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing api key, got %d", resp.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/api/v1/tenants", nil)
	req.Header.Set("X-API-Key", "sfk_acme_dev")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with admin api key, got %d", resp.StatusCode)
	}
}

func tenantIDFor(t *testing.T, srv *api.Server, slug string) string {
	t.Helper()
	tnt, err := store.GetTenantBySlug(context.Background(), srv.DB().Admin, slug)
	if err != nil {
		t.Fatalf("lookup tenant %s: %v", slug, err)
	}
	return tnt.ID
}
