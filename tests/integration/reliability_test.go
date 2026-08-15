//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"syncforge/internal/events"
	"syncforge/internal/simulator"
	"syncforge/internal/store"
)

// apiKeyReq builds an authenticated request against the SyncForge API.
func apiKeyReq(t *testing.T, path string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-API-Key", "sfk_acme_dev")
	return req
}

func dlqItemFor(t *testing.T, h *pipelineHarness, eventID string) store.DeadLetter {
	t.Helper()
	items, err := store.ListDeadLetters(context.Background(), h.db.App, h.acmeID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range items {
		if it.EventID == eventID {
			return it
		}
	}
	t.Fatalf("dead letter for event %s not found", eventID)
	return store.DeadLetter{}
}

func retryRowExists(t *testing.T, h *pipelineHarness, eventID string) bool {
	t.Helper()
	var n int
	err := h.db.Admin.QueryRow(context.Background(),
		`SELECT count(*) FROM retry_queue WHERE tenant_id=$1 AND event_id=$2`, h.acmeID, eventID).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n > 0
}

// TestProviderOutageIsDurableAndRecovers proves that a sustained provider
// outage does not lose events: the failed event is held durably (source_events
// marked failed + a retry queued) and once the provider recovers, the retry
// machinery applies it exactly once.
func TestProviderOutageIsDurableAndRecovers(t *testing.T) {
	h := newPipelineHarness(t)

	// Provider outage: every HubSpot API call fails with a transient 500.
	h.hubSrv.Faults.Set(simulator.FaultConfig{FailureRate: 1.0})

	createWebhook := `{
		"event_id": "sf-outage-1",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000101",
		"event_type": "created",
		"source_version": 1,
		"occurred_at": "2024-01-01T00:00:00Z",
		"payload": {"fields": {"id": "sf-000101", "first_name": "Durable", "last_name": "Outage",
			"email": "outage@example.com", "phone": "+1-555-0101", "company": "Cloud"}}
	}`
	resp, _ := postWebhook(t, h.api, "sfs-dev-secret", createWebhook)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("webhook status %d", resp.StatusCode)
	}

	// The worker must fail against the down provider and durably queue a retry.
	waitFor(t, 10*time.Second, "retry queued during outage", func() bool {
		return retryRowExists(t, h, "sf-outage-1")
	})

	// Event stays durable as 'failed' while the provider is down.
	ev, err := store.GetSourceEvent(context.Background(), h.db.App, h.acmeID, "sf-outage-1")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != "failed" {
		t.Fatalf("expected source event status 'failed' during outage, got %q", ev.Status)
	}

	// Provider recovers.
	h.hubSrv.Faults.Set(simulator.FaultConfig{})
	h.drainRetries(t, 15*time.Second, "event synchronizes after recovery", func() bool {
		return findHub(h.hubContacts(t), func(r map[string]any) bool {
			return r["emailAddress"] == "outage@example.com"
		}) != nil
	})

	// Exactly one destination record, no duplicates from retries.
	if n := len(h.hubContacts(t)); n != 1 {
		t.Fatalf("expected exactly 1 hubspot contact after recovery, got %d", n)
	}
	if retryRowExists(t, h, "sf-outage-1") {
		t.Fatal("retry row should be removed after success")
	}
	if _, err := store.GetCanonicalByProvider(context.Background(), h.db.App, h.acmeID, "customer", "salesforce", "sf-000101"); err != nil {
		t.Fatalf("canonical record missing after recovery: %v", err)
	}
	items, _ := store.ListDeadLetters(context.Background(), h.db.App, h.acmeID, "", 100)
	if len(items) != 0 {
		t.Fatalf("unexpected dead letters after recovery: %+v", items)
	}
}

// TestWorkerFailureReleasesClaimAndRedeliveryIsSafe proves the worker-crash
// semantics: a failed attempt releases its idempotency claim (so retries can
// re-run), no duplicate destination mutation occurs even when the same logical
// event is delivered again, and recovery yields exactly one record.
func TestWorkerFailureReleasesClaimAndRedeliveryIsSafe(t *testing.T) {
	h := newPipelineHarness(t)

	h.hubSrv.Faults.Set(simulator.FaultConfig{FailureRate: 1.0})

	event := map[string]any{
		"event_id":       "sf-crash-1",
		"tenant_id":      h.acmeID,
		"source":         "salesforce",
		"entity_type":    "customer",
		"entity_id":      "sf-000202",
		"event_type":     "created",
		"source_version": 1,
		"payload": map[string]any{"fields": map[string]any{
			"id": "sf-000202", "first_name": "Crash", "last_name": "Safe",
			"email": "crash@example.com", "phone": "+1-555-0202", "company": "X",
		}},
	}
	value, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte(h.acmeID + ":customer:sf-000202")

	// Deliver once and let it fail against the down provider.
	if err := h.bus.Publish(context.Background(), "sync.events", key, value); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "retry queued after crash", func() bool {
		return retryRowExists(t, h, "sf-crash-1")
	})

	// The failure must have released the claim (so a retry can re-run)...
	if _, ok, err := store.ProcessedEventAt(context.Background(), h.db.App, h.acmeID, "salesforce", "sf-crash-1"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("claim must be released after a failed attempt")
	}

	// ...and repeated redelivery of the same logical event does not create
	// destination rows while the provider is down.
	for i := 0; i < 3; i++ {
		if err := h.bus.Publish(context.Background(), "sync.events", key, value); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(1 * time.Second)
	if n := h.hubSrv.Store.Count(); n != 0 {
		t.Fatalf("no records may be created while provider is down, got %d", n)
	}

	// Provider recovers; retries converge to exactly one destination mutation.
	h.hubSrv.Faults.Set(simulator.FaultConfig{})
	h.drainRetries(t, 15*time.Second, "event eventually applied once", func() bool {
		return findHub(h.hubContacts(t), func(r map[string]any) bool {
			return r["emailAddress"] == "crash@example.com"
		}) != nil
	})
	if n := len(h.hubContacts(t)); n != 1 {
		t.Fatalf("expected exactly 1 hubspot contact after recovery, got %d", n)
	}
}

// TestSchemaErrorGoesStraightToDLQ proves a permanent failure (schema
// validation) skips the retry queue and is parked in the DLQ, then can be
// inspected and discarded via the API.
func TestSchemaErrorGoesStraightToDLQ(t *testing.T) {
	h := newPipelineHarness(t)

	// Missing last_name -> adapter.Validate fails (SCHEMA_ERROR, permanent).
	schemaWebhook := `{
		"event_id": "sf-schema-1",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000303",
		"event_type": "created",
		"source_version": 1,
		"occurred_at": "2024-01-01T00:00:00Z",
		"payload": {"fields": {"id": "sf-000303", "first_name": "NoLast",
			"email": "schema@example.com", "phone": "+1-555-0303", "company": "X"}}
	}`
	resp, _ := postWebhook(t, h.api, "sfs-dev-secret", schemaWebhook)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("webhook status %d", resp.StatusCode)
	}

	waitFor(t, 10*time.Second, "event dead-lettered", func() bool {
		items, err := store.ListDeadLetters(context.Background(), h.db.App, h.acmeID, "", 100)
		if err != nil {
			return false
		}
		return len(items) == 1
	})

	item := dlqItemFor(t, h, "sf-schema-1")
	if item.Status != "open" {
		t.Fatalf("expected dlq status open, got %s", item.Status)
	}
	if item.ErrorClass != "SCHEMA_ERROR" {
		t.Fatalf("expected error_class SCHEMA_ERROR, got %s", item.ErrorClass)
	}

	ev, err := store.GetSourceEvent(context.Background(), h.db.App, h.acmeID, "sf-schema-1")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != "dlq" {
		t.Fatalf("expected source event status dlq, got %q", ev.Status)
	}
	if retryRowExists(t, h, "sf-schema-1") {
		t.Fatal("permanent failures must not be retried")
	}

	// Operator discards it through the API.
	req := apiKeyReq(t, h.api.URL+"/api/v1/dlq/"+item.ID+"/discard")
	req.Method = http.MethodPost
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("discard status %d", resp2.StatusCode)
	}

	items, _ := store.ListDeadLetters(context.Background(), h.db.App, h.acmeID, "", 100)
	for _, it := range items {
		if it.ID == item.ID && it.Status != "discarded" {
			t.Fatalf("expected dlq status discarded, got %s", it.Status)
		}
	}
}

// TestRetryExhaustionThenManualReplay proves the full DLQ lifecycle: transient
// failures retry with backoff until attempts are exhausted, the event lands in
// the DLQ, and an operator replay drives it to success exactly once.
func TestRetryExhaustionThenManualReplay(t *testing.T) {
	h := newPipelineHarness(t)

	h.hubSrv.Faults.Set(simulator.FaultConfig{FailureRate: 1.0})

	createWebhook := `{
		"event_id": "sf-dlq-full",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000404",
		"event_type": "created",
		"source_version": 1,
		"occurred_at": "2024-01-01T00:00:00Z",
		"payload": {"fields": {"id": "sf-000404", "first_name": "Retry", "last_name": "Cycle",
			"email": "cycle@example.com", "phone": "+1-555-0404", "company": "Z"}}
	}`
	resp, _ := postWebhook(t, h.api, "sfs-dev-secret", createWebhook)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("webhook status %d", resp.StatusCode)
	}

	// Retries exhaust against the down provider and the event is parked in DLQ.
	h.transientToDLQ(t, "sf-dlq-full")
	item := dlqItemFor(t, h, "sf-dlq-full")
	if item.Status != "open" {
		t.Fatalf("expected dlq open after exhaustion, got %s", item.Status)
	}
	if n := h.hubSrv.Store.Count(); n != 0 {
		t.Fatalf("no records may exist while provider is down, got %d", n)
	}

	// Provider recovers; operator replays from the DLQ.
	h.hubSrv.Faults.Set(simulator.FaultConfig{})
	req := apiKeyReq(t, h.api.URL+"/api/v1/dlq/"+item.ID+"/retry")
	req.Method = http.MethodPost
	r2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Body.Close()
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("dlq retry status %d", r2.StatusCode)
	}

	h.drainRetries(t, 15*time.Second, "replayed event succeeds", func() bool {
		return findHub(h.hubContacts(t), func(r map[string]any) bool {
			return r["emailAddress"] == "cycle@example.com"
		}) != nil
	})

	if n := len(h.hubContacts(t)); n != 1 {
		t.Fatalf("expected exactly 1 hubspot contact after replay, got %d", n)
	}
	items, _ := store.ListDeadLetters(context.Background(), h.db.App, h.acmeID, "", 100)
	for _, it := range items {
		if it.ID == item.ID && it.Status != "resolved" {
			t.Fatalf("expected dlq resolved after replay, got %s", it.Status)
		}
	}
	if retryRowExists(t, h, "sf-dlq-full") {
		t.Fatal("retry row should be gone after success")
	}
}

// transientToDLQ drives the retry engine until the event exhausts its attempts
// and lands in the dead-letter queue.
func (h *pipelineHarness) transientToDLQ(t *testing.T, eventID string) {
	t.Helper()
	h.drainRetries(t, 20*time.Second, "event exhausted into DLQ", func() bool {
		items, err := store.ListDeadLetters(context.Background(), h.db.App, h.acmeID, "", 100)
		if err != nil {
			return false
		}
		for _, it := range items {
			if it.EventID == eventID && it.Status == "open" {
				return true
			}
		}
		return false
	})
}

// TestSyncJobCheckpointResume proves initial full synchronization is resumable:
// a job whose worker crashed after applying page 1 (2 records) resumes from its
// saved cursor, finishes the remaining records, and never duplicates the ones
// already applied.
func TestSyncJobCheckpointResume(t *testing.T) {
	h := newPipelineHarness(t)

	// Seed 6 customers in Salesforce with sf- prefixed ids.
	h.sfSrv.Store.SetIDPrefix("sf-")
	h.sfSrv.Store.Seed(6, func(id string, n int) map[string]any {
		return map[string]any{
			"first_name": "Full", "last_name": "Sync",
			"email": fmt.Sprintf("fullsync-%d@example.com", n),
			"phone": "+1-555-1000", "company": "Seed",
		}
	})

	job, err := store.CreateSyncJob(context.Background(), h.db.App, store.SyncJob{
		TenantID: h.acmeID, Entity: "customer", Source: "salesforce", Destination: "hubspot",
		BatchSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Page 1 (records sf-000001..sf-000002) is genuinely applied through the
	// same worker the runner uses, using the runner's deterministic event ids
	// ("jobsync:<job>:<record>"), exactly as a non-crashed first loop would.
	for _, recordID := range []string{"sf-000001", "sf-000002"} {
		applySyncJobEvent(t, h, job, recordID)
	}

	// Simulate a crash after page 1: the runner checkpoints the cursor + count
	// to the DB, then dies before listing the next page.
	if _, err := h.db.Admin.Exec(context.Background(),
		`UPDATE sync_jobs SET status='running', started_at=now() - interval '2 minutes', cursor='sf-000002', processed=2
		 WHERE id=$1`, job.ID); err != nil {
		t.Fatal(err)
	}

	// The runner adopts the stale-stuck job and resumes from its cursor.
	claimed, err := store.ClaimNextSyncJob(context.Background(), h.db.Admin)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != job.ID {
		t.Fatalf("claimed job %s, want %s", claimed.ID, job.ID)
	}
	if err := h.syncRunner.Execute(context.Background(), claimed); err != nil {
		t.Fatalf("run sync job: %v", err)
	}

	// All 6 records end up in HubSpot, exactly once.
	waitFor(t, 10*time.Second, "full sync contacts appear", func() bool {
		return len(h.hubContacts(t)) == 6
	})
	if n := len(h.hubContacts(t)); n != 6 {
		t.Fatalf("expected 6 hubspot contacts after full sync, got %d", n)
	}

	resumed, err := store.GetSyncJob(context.Background(), h.db.App, h.acmeID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Status != "completed" {
		t.Fatalf("expected job completed, got %s", resumed.Status)
	}
	if resumed.Processed != 6 {
		t.Fatalf("expected processed=6, got %d", resumed.Processed)
	}
}

// applySyncJobEvent applies one full-sync record through the worker, cloning
// exactly the canonical event the runner builds (so the idempotency claim keys
// match and a resume skips it).
func applySyncJobEvent(t *testing.T, h *pipelineHarness, job store.SyncJob, recordID string) {
	t.Helper()
	rec, ok := h.sfSrv.Store.Get(recordID)
	if !ok {
		t.Fatalf("seeded record %s missing", recordID)
	}
	ev := &events.Event{
		EventID:       "jobsync:" + job.ID + ":" + recordID,
		TenantID:      job.TenantID,
		Source:        job.Source,
		EntityType:    "customer",
		EntityID:      rec.ID,
		EventType:     events.EventCreated,
		SourceVersion: 1,
		OccurredAt:    time.Now().UTC(),
		ReceivedAt:    time.Now().UTC(),
		Provenance:    events.Provenance{OriginSource: job.Source, SyncOperationID: job.ID},
		Payload:       map[string]any{"fields": rec.Data},
	}
	if err := h.worker.Process(context.Background(), ev); err != nil {
		t.Fatalf("apply page-1 record %s: %v", recordID, err)
	}
}
