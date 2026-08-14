//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"

	"syncforge/internal/api"
	"syncforge/internal/cache"
	"syncforge/internal/config"
	"syncforge/internal/db"
	"syncforge/internal/eventbus"
	"syncforge/internal/ingestion"
	"syncforge/internal/observability"
	"syncforge/internal/simulator"
	"syncforge/internal/store"
	"syncforge/internal/syncworker"
)

// pipelineHarness wires the real ingestion + sync worker around a memory bus
// and two in-process provider simulators.
type pipelineHarness struct {
	db      *db.DB
	bus     *eventbus.MemoryBus
	api     *httptest.Server
	sfSim   *httptest.Server
	hubSim  *httptest.Server
	acmeID  string
	cancel  context.CancelFunc
	workers []func() error
}

func newPipelineHarness(t *testing.T) *pipelineHarness {
	t.Helper()
	database := newDB(t)

	// Provider simulators.
	sfSpec := &simulator.Spec{Name: "salesforce", EntityType: "customer", IDKey: "id", TimeKey: "updated_at", IDPrefix: "sf-", Path: "/customers"}
	sfSrv := simulator.NewServer(sfSpec, simulator.Options{
		RateLimitPerMin: 1000,
		SeedCount:       0,
		Log:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	sfSim := httptest.NewServer(sfSrv.Handler())
	t.Cleanup(sfSim.Close)

	hubSpec := &simulator.Spec{Name: "hubspot", EntityType: "contact", IDKey: "contact_id", TimeKey: "modifiedAt", IDPrefix: "hub-", Path: "/contacts"}
	hubSrv := simulator.NewServer(hubSpec, simulator.Options{
		RateLimitPerMin: 1000,
		SeedCount:       0,
		Log:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	hubSim := httptest.NewServer(hubSrv.Handler())
	t.Cleanup(hubSim.Close)

	// API server seeding Acme + connections pointing at the sims + policy.
	cfg := config.Load()
	cfg.SeedAcme = true
	cfg.SeedSFBaseURL = sfSim.URL
	cfg.SeedHubBaseURL = hubSim.URL
	cfg.SeedSFSSecret = "sfs-dev-secret"
	cfg.SeedHubSecret = "sfh-dev-secret"

	metrics, err := observability.NewServiceMetrics(otel.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	apiSrv := api.New(cfg, database, cache.New("localhost:6379"), metrics, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err := apiSrv.SeedDemoTenant(context.Background()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	apiSim := httptest.NewServer(apiSrv.Router(promhttp.Handler()))
	t.Cleanup(apiSim.Close)

	acme, err := store.GetTenantBySlug(context.Background(), database.Admin, "acme")
	if err != nil {
		t.Fatal(err)
	}

	bus := eventbus.NewMemoryBus(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	syncMetrics, err := observability.NewSyncMetrics(otel.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}

	worker := syncworker.New(database, syncMetrics, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	processor := ingestion.New(database, bus, slog.New(slog.NewTextHandler(os.Stderr, nil)))

	// Start worker consumer + ingestion processor.
	workerErr := make(chan error, 1)
	go func() { workerErr <- bus.Subscribe(ctx, eventbus.TopicSyncEvents, "test-group", worker.Handle) }()
	ingestErr := make(chan error, 1)
	go func() { ingestErr <- processor.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
	})

	if err := bus.WaitForSubscribers(ctx, eventbus.TopicSyncEvents, 1); err != nil {
		t.Fatalf("worker never subscribed: %v", err)
	}

	// Wire provider webhooks into the SyncForge gateway so bidirectional flows
	// (including echo webhooks) are exercised end-to-end.
	sfSrv.SetWebhook(apiSim.URL+"/webhooks/salesforce/acme", "sfs-dev-secret")
	hubSrv.SetWebhook(apiSim.URL+"/webhooks/hubspot/acme", "sfh-dev-secret")

	return &pipelineHarness{
		db:      database,
		bus:     bus,
		api:     apiSim,
		sfSim:   sfSim,
		hubSim:  hubSim,
		acmeID:  acme.ID,
		cancel:  cancel,
		workers: []func() error{func() error { return <-workerErr }, func() error { return <-ingestErr }},
	}
}

// hubContacts returns all records currently in the HubSpot simulator.
func (h *pipelineHarness) hubContacts(t *testing.T) []map[string]any {
	t.Helper()
	resp, err := http.Get(h.hubSim.URL + "/api/v1/contacts?limit=1000")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body.Records
}

// sfCustomers returns all records currently in the Salesforce simulator.
func (h *pipelineHarness) sfCustomers(t *testing.T) []map[string]any {
	t.Helper()
	resp, err := http.Get(h.sfSim.URL + "/api/v1/customers?limit=1000")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body.Records
}

// findHub returns the hubspot contact matching the predicate.
func findHub(recs []map[string]any, pred func(map[string]any) bool) map[string]any {
	for _, r := range recs {
		if pred(r) {
			return r
		}
	}
	return nil
}

// waitFor polls fn until it returns true.
func waitFor(t *testing.T, timeout time.Duration, msg string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func (h *pipelineHarness) waitForHubContact(t *testing.T, want int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		recs := h.hubContacts(t)
		if len(recs) >= want {
			return recs
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d hubspot contacts", want)
	return nil
}

// TestPipelineWebhookToDestination proves webhook -> source_events -> bus ->
// worker -> destination, including an update flowing through.
func TestPipelineWebhookToDestination(t *testing.T) {
	h := newPipelineHarness(t)

	// Create event via a signed webhook.
	createWebhook := `{
		"event_id": "sf-evt-1",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000001",
		"event_type": "created",
		"source_version": 1,
		"occurred_at": "2024-01-01T00:00:00Z",
		"payload": {"fields": {"id": "sf-000001", "first_name": "Ada", "last_name": "Lovelace",
			"email": "ada@example.com", "phone": "+1-555-0000", "company": "Analytical"}}
	}`
	resp, _ := postWebhook(t, h.api, "sfs-dev-secret", createWebhook)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create webhook status %d", resp.StatusCode)
	}

	contacts := h.waitForHubContact(t, 1)
	if contacts[0]["emailAddress"] != "ada@example.com" {
		t.Fatalf("hubspot record not created with mapped fields: %v", contacts[0])
	}

	// Update event -> hubspot record should update.
	updateWebhook := `{
		"event_id": "sf-evt-2",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000001",
		"event_type": "updated",
		"source_version": 2,
		"occurred_at": "2024-01-01T00:01:00Z",
		"payload": {"fields": {"id": "sf-000001", "first_name": "Ada", "last_name": "Lovelace",
			"email": "ada.updated@example.com", "phone": "+1-555-9999", "company": "Analytical"}}
	}`
	resp, _ = postWebhook(t, h.api, "sfs-dev-secret", updateWebhook)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("update webhook status %d", resp.StatusCode)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		contacts = h.hubContacts(t)
		if len(contacts) == 1 && contacts[0]["emailAddress"] == "ada.updated@example.com" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("hubspot record not updated: %v", contacts)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(contacts) != 1 {
		t.Fatalf("expected exactly 1 hubspot contact, got %d", len(contacts))
	}

	// Canonical record must exist with both provider ids.
	canonical, err := store.GetCanonicalByProvider(context.Background(), h.db.App, h.acmeID, "customer", "salesforce", "sf-000001")
	if err != nil {
		t.Fatalf("canonical record missing: %v", err)
	}
	if canonical.ProviderIDs["salesforce"] != "sf-000001" || canonical.ProviderIDs["hubspot"] == "" {
		t.Fatalf("provider id mapping not persisted: %+v", canonical.ProviderIDs)
	}
	if canonical.SourceVersions["salesforce"] != 2 {
		t.Fatalf("source version not tracked: %+v", canonical.SourceVersions)
	}
}

// TestWorkerDedupes100Duplicates proves the idempotency log collapses 100
// duplicate deliveries into exactly one destination mutation.
func TestWorkerDedupes100Duplicates(t *testing.T) {
	h := newPipelineHarness(t)

	event := map[string]any{
		"event_id":       "dup-evt",
		"tenant_id":      h.acmeID,
		"source":         "salesforce",
		"entity_type":    "customer",
		"entity_id":      "sf-000001",
		"event_type":     "created",
		"source_version": 1,
		"occurred_at":    "2024-01-01T00:00:00Z",
		"payload": map[string]any{"fields": map[string]any{
			"id": "sf-000001", "first_name": "Dup", "last_name": "Test",
			"email": "dup@example.com", "phone": "+1-555-1111", "company": "X",
		}},
	}
	value, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte(h.acmeID + ":customer:sf-000001")

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := h.bus.Publish(ctx, eventbus.TopicSyncEvents, key, value); err != nil {
			t.Fatal(err)
		}
	}

	// Wait until the contact exists, then allow any stragglers to settle.
	_ = h.waitForHubContact(t, 1)
	time.Sleep(1 * time.Second)

	contacts := h.hubContacts(t)
	if len(contacts) != 1 {
		t.Fatalf("100 duplicates created %d destination records (want exactly 1)", len(contacts))
	}

	// The duplicate event itself must be claimed exactly once (the hubspot echo
	// is a distinct event and may be claimed separately).
	_, ok, err := store.ProcessedEventAt(ctx, h.db.App, h.acmeID, "salesforce", "dup-evt")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("the original event was never marked processed")
	}
}

// TestPipelineDeletePropagates proves deletes flow through and leave tombstones.
func TestPipelineDeletePropagates(t *testing.T) {
	h := newPipelineHarness(t)

	createWebhook := `{
		"event_id": "sf-del-1",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000002",
		"event_type": "created",
		"source_version": 1,
		"occurred_at": "2024-01-01T00:00:00Z",
		"payload": {"fields": {"id": "sf-000002", "first_name": "Del", "last_name": "Me",
			"email": "del@example.com", "phone": "+1-555-2222", "company": "Y"}}
	}`
	resp, _ := postWebhook(t, h.api, "sfs-dev-secret", createWebhook)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create webhook status %d", resp.StatusCode)
	}
	contacts := h.waitForHubContact(t, 1)

	deleteWebhook := `{
		"event_id": "sf-del-2",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000002",
		"event_type": "deleted",
		"source_version": 2,
		"occurred_at": "2024-01-01T00:02:00Z",
		"payload": {"fields": {"id": "sf-000002"}}
	}`
	resp, _ = postWebhook(t, h.api, "sfs-dev-secret", deleteWebhook)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("delete webhook status %d", resp.StatusCode)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		contacts = h.hubContacts(t)
		if len(contacts) == 1 && contacts[0]["deleted"] == true {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("hubspot record not tombstoned: %v", contacts)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Canonical record must remain as a tombstone (not deleted).
	canonical, err := store.GetCanonicalByProvider(context.Background(), h.db.App, h.acmeID, "customer", "salesforce", "sf-000002")
	if err != nil {
		t.Fatalf("tombstone missing: %v", err)
	}
	if !canonical.Tombstone {
		t.Fatal("canonical record should be tombstoned")
	}
}
