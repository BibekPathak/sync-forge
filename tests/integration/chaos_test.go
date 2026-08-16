//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"syncforge/internal/simulator"
	"syncforge/internal/store"
	"syncforge/internal/syncworker"
	"syncforge/load_test"
)

// TestFaultHangTimesOutAndRecovers proves a hung provider produces a durable
// TRANSIENT failure (client timeout) that the retry machinery replays to
// success exactly once once the provider responds again.
func TestFaultHangTimesOutAndRecovers(t *testing.T) {
	h := newPipelineHarness(t)

	// Shorten the connector timeout so a hang trips it quickly instead of
	// waiting the 15s registry default.
	h.worker.WithOptions(syncworker.Options{ConnectorTimeout: 300 * time.Millisecond})

	// HubSpot hangs on every request for far longer than the client timeout.
	h.hubSrv.Faults.Set(simulator.FaultConfig{HangMS: 5000, HangPercent: 1.0})

	createWebhook := `{
		"event_id": "hang-1",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000501",
		"event_type": "created",
		"source_version": 1,
		"occurred_at": "2024-01-01T00:00:00Z",
		"payload": {"fields": {"id": "sf-000501", "first_name": "Hung", "last_name": "Provider",
			"email": "hung@example.com", "phone": "+1-555-0501", "company": "Slow"}}
	}`
	resp, _ := postWebhook(t, h.api, "sfs-dev-secret", createWebhook)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("webhook status %d", resp.StatusCode)
	}

	// The timeout must be classified TRANSIENT and durably queued for retry.
	waitFor(t, 10*time.Second, "timeout retry queued", func() bool {
		return retryRowExists(t, h, "hang-1")
	})

	// Provider recovers; the retry replays the event to exactly one record.
	h.hubSrv.Faults.Set(simulator.FaultConfig{})
	h.drainRetries(t, 20*time.Second, "event applied after hang recovery", func() bool {
		return findHub(h.hubContacts(t), func(r map[string]any) bool {
			return r["emailAddress"] == "hung@example.com"
		}) != nil
	})
	if n := len(h.hubContacts(t)); n != 1 {
		t.Fatalf("expected exactly 1 hubspot contact after hang recovery, got %d", n)
	}
	if retryRowExists(t, h, "hang-1") {
		t.Fatal("retry row should be gone after success")
	}
}

// TestFaultCorruptionSkipsInvalidRecordsDuringReconcile proves partial payload
// corruption (a dropped required field) is tolerated by the pipeline: a
// reconcile sweep counts the corrupted record as failed instead of crashing or
// aborting, and a clean sweep afterward reconciles normally.
func TestFaultCorruptionSkipsInvalidRecordsDuringReconcile(t *testing.T) {
	h := newPipelineHarness(t)

	// Create a normal record so hubspot has a contact to reconcile.
	createWebhook := `{
		"event_id": "corrupt-1",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000601",
		"event_type": "created",
		"source_version": 1,
		"occurred_at": "2024-01-01T00:00:00Z",
		"payload": {"fields": {"id": "sf-000601", "first_name": "Corrupt", "last_name": "Field",
			"email": "corrupt@example.com", "phone": "+1-555-0601", "company": "X"}}
	}`
	resp, _ := postWebhook(t, h.api, "sfs-dev-secret", createWebhook)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("webhook status %d", resp.StatusCode)
	}
	_ = h.waitForHubContact(t, 1)

	// Drop a required field on the source being reconciled (hubspot).
	h.hubSrv.Faults.Set(simulator.FaultConfig{DropField: "lastName"})

	run := startReconcile(t, h, "hubspot", "manual")
	if run.Status != store.ReconcileComplete {
		t.Fatalf("expected reconcile run to complete despite corruption, got %s", run.Status)
	}
	job, err := store.GetSyncJob(context.Background(), h.db.App, h.acmeID, *run.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Failed < 1 {
		t.Fatalf("expected the corrupted record counted as failed, got %d", job.Failed)
	}

	// A clean sweep sees the record as in sync (no findings).
	h.hubSrv.Faults.Set(simulator.FaultConfig{})
	run2 := startReconcile(t, h, "hubspot", "manual")
	if run2.Status != store.ReconcileComplete {
		t.Fatalf("clean reconcile should complete, got %s", run2.Status)
	}
	if len(findingsFor(t, h, run2.ID)) != 0 {
		t.Fatalf("expected no findings after clean sweep, got %d", len(findingsFor(t, h, run2.ID)))
	}
}

// TestChaosScriptedScenario runs a scripted sequence of faults and recovers
// between each: healthy -> hard outage -> recover -> rate limit -> recover.
// Each phase asserts the invariant (zero data loss, no duplicates) so the
// sequence proves the pipeline survives a realistic incident lifecycle.
func TestChaosScriptedScenario(t *testing.T) {
	h := newPipelineHarness(t)

	expected := 0
	phase := func(name string, events int, faults simulator.FaultConfig, recoverFaults simulator.FaultConfig) {
		t.Helper()
		t.Logf("phase: %s (events=%d)", name, events)
		h.hubSrv.Faults.Set(faults)

		gen := &loadtest.Generator{
			URL:           h.api.URL,
			WebhookSecret: "sfs-dev-secret",
			Source:        "salesforce",
			TenantSlug:    "acme",
		}
		res := gen.Burst(context.Background(), events, 16, "chaos-"+name, func(i int) map[string]any { return nil })
		t.Logf("  burst: %s", res.String())
		if res.Accepted != events {
			t.Fatalf("%s: expected all accepted, got %d/%d", name, res.Accepted, events)
		}
		expected += events

		if recoverFaults != (simulator.FaultConfig{}) {
			h.hubSrv.Faults.Set(recoverFaults)
		} else {
			h.hubSrv.Faults.Set(simulator.FaultConfig{})
		}

		// Drain until every event sent so far has landed in hubspot.
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			if len(h.hubContacts(t)) >= expected {
				break
			}
			h.retryEngine.Drain(context.Background())
			time.Sleep(25 * time.Millisecond)
		}
		if got := len(h.hubContacts(t)); got != expected {
			t.Fatalf("%s: expected %d contacts, got %d", name, expected, got)
		}
		assertNoDuplicateContacts(t, h)
	}

	// Phase 1: healthy baseline.
	phase("healthy", 40, simulator.FaultConfig{}, simulator.FaultConfig{})
	// Phase 2: hard outage (every write fails transiently).
	phase("outage", 40, simulator.FaultConfig{FailureRate: 1.0}, simulator.FaultConfig{})
	// Phase 3: rate-limited (bucket restores after recovery).
	phase("rate-limit", 40, simulator.FaultConfig{RateLimitPerMin: 2}, simulator.FaultConfig{RateLimitPerMin: 1000})

	// Final: no lingering retries or DLQ entries.
	waitFor(t, 10*time.Second, "retries drained", func() bool {
		var n int
		err := h.db.Admin.QueryRow(context.Background(),
			`SELECT count(*) FROM retry_queue WHERE tenant_id=$1`, h.acmeID).Scan(&n)
		if err != nil {
			return false
		}
		return n == 0
	})
	dlq, err := store.ListDeadLetters(context.Background(), h.db.App, h.acmeID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlq) != 0 {
		t.Fatalf("expected no DLQ entries after chaos scenario, got %d", len(dlq))
	}
}

// assertNoDuplicateContacts fails when any two hubspot contacts share an email.
func assertNoDuplicateContacts(t *testing.T, h *pipelineHarness) {
	t.Helper()
	seen := make(map[string]bool)
	for _, c := range h.hubContacts(t) {
		email, _ := c["emailAddress"].(string)
		if email == "" {
			t.Fatalf("contact missing emailAddress: %v", c)
		}
		if seen[email] {
			t.Fatalf("duplicate destination record for email %s", email)
		}
		seen[email] = true
	}
}
