//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"syncforge/internal/store"
)

// startReconcile creates a reconciliation run through the API and executes its
// scheduling job synchronously through the runner, returning the finished run.
func startReconcile(t *testing.T, h *pipelineHarness, source, mode string) store.ReconciliationRun {
	t.Helper()
	body, err := json.Marshal(map[string]any{"source": source, "mode": mode})
	if err != nil {
		t.Fatal(err)
	}
	req := apiKeyReq(t, h.api.URL+"/api/v1/reconciliations")
	rb, err := http.NewRequest(http.MethodPost, req.URL.String(), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rb.Header.Set("Content-Type", "application/json")
	rb.Header.Set("X-API-Key", "sfk_acme_dev")
	resp, err := http.DefaultClient.Do(rb)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create reconciliation status %d", resp.StatusCode)
	}
	var run store.ReconciliationRun
	if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
		t.Fatal(err)
	}
	if run.JobID == nil {
		t.Fatal("reconcile run has no job id")
	}
	job, err := store.GetSyncJob(context.Background(), h.db.App, h.acmeID, *run.JobID)
	if err != nil {
		t.Fatalf("load reconcile job: %v", err)
	}
	if err := h.syncRunner.Execute(context.Background(), job); err != nil {
		t.Fatalf("execute reconcile job: %v", err)
	}
	finished, err := store.GetReconciliationRun(context.Background(), h.db.App, h.acmeID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	return finished
}

// findingsFor lists all findings for a run.
func findingsFor(t *testing.T, h *pipelineHarness, runID string) []store.ReconciliationFinding {
	t.Helper()
	items, err := store.ListReconciliationFindings(context.Background(), h.db.App, h.acmeID, runID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	return items
}

// findingOf returns the single finding matching the predicate.
func findingOf(t *testing.T, h *pipelineHarness, runID, kind string) store.ReconciliationFinding {
	t.Helper()
	for _, f := range findingsFor(t, h, runID) {
		if f.Kind == kind {
			return f
		}
	}
	t.Fatalf("no finding of kind %s in run %s", kind, runID)
	return store.ReconciliationFinding{}
}

// TestReconcileAutoRepairsDrift proves an auto-mode run pushes canonical state
// back to a provider whose record drifted out-of-band: the provider converges,
// the canonical record is untouched, and the echo of our own repair write is
// recognized and dropped (no duplicate propagation).
func TestReconcileAutoRepairsDrift(t *testing.T) {
	h := newPipelineHarness(t)
	ids := createSFAndPropagate(t, h, "sf-000001", "Ada", "Lovelace", "ada.recon@example.com")

	// Simulate an out-of-band drift: mutate the provider directly, bypassing
	// webhooks, so the canonical model and HubSpot are unaware.
	h.sfSrv.Store.Update(fmt.Sprint(ids["sf_id"]), map[string]any{"first_name": "Drifted"})

	run := startReconcile(t, h, "salesforce", "auto")
	if run.Status != store.ReconcileComplete {
		t.Fatalf("run not completed: %s", run.Status)
	}
	if run.Drift != 1 {
		t.Fatalf("expected 1 drift finding, got %d", run.Drift)
	}
	f := findingOf(t, h, run.ID, store.FindingDrift)
	if f.Status != store.FindingApplied {
		t.Fatalf("drift finding not applied: %s", f.Status)
	}

	// The provider must converge back to canonical state.
	sf := sfCustomerByEmail(t, h, "ada.recon@example.com")
	if first := fmt.Sprint(sf["first_name"]); first != "Ada" {
		t.Fatalf("provider not repaired: first_name=%q", first)
	}

	// Canonical fields must be unchanged (still the original writer's state).
	canonical, err := store.GetCanonicalByProvider(context.Background(), h.db.App, h.acmeID, "customer", "salesforce", fmt.Sprint(ids["sf_id"]))
	if err != nil {
		t.Fatal(err)
	}
	if first := fmt.Sprint(canonical.Fields["first_name"]); first != "Ada" {
		t.Fatalf("canonical drifted: first_name=%q", first)
	}

	// Exactly one record on each side (our repair write echoed back and was
	// dropped, not propagated as a second mutation).
	time.Sleep(500 * time.Millisecond)
	if n := len(h.hubContacts(t)); n != 1 {
		t.Fatalf("expected 1 hubspot contact, got %d", n)
	}
	if n := len(h.sfCustomers(t)); n != 1 {
		t.Fatalf("expected 1 salesforce customer, got %d", n)
	}
}

// TestReconcileAutoDeletesTombstonedProviderRecord proves a tombstoned
// canonical record whose provider still serves a live record is deleted on the
// provider by an auto run.
func TestReconcileAutoDeletesTombstonedProviderRecord(t *testing.T) {
	h := newPipelineHarness(t)
	ids := createSFAndPropagate(t, h, "sf-000002", "Del", "Recon", "del.recon@example.com")

	// Tombstone the canonical record via a delete webhook sent directly to the
	// gateway. The Salesforce simulator never saw the delete, so its record
	// stays live: exactly the straggler a reconcile run should clean up.
	delWebhook := `{
		"event_id": "sf-rec-del",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "` + fmt.Sprint(ids["sf_id"]) + `",
		"event_type": "deleted",
		"source_version": 2,
		"occurred_at": "2024-01-01T00:02:00Z",
		"payload": {"fields": {"id": "` + fmt.Sprint(ids["sf_id"]) + `"}}
	}`
	resp, _ := postWebhook(t, h.api, "sfs-dev-secret", delWebhook)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("delete webhook status %d", resp.StatusCode)
	}
	// Wait for the tombstone to land.
	waitFor(t, 10*time.Second, "canonical tombstoned", func() bool {
		c, err := store.GetCanonicalByProvider(context.Background(), h.db.App, h.acmeID, "customer", "salesforce", fmt.Sprint(ids["sf_id"]))
		return err == nil && c.Tombstone
	})

	run := startReconcile(t, h, "salesforce", "auto")
	if run.Status != store.ReconcileComplete {
		t.Fatalf("run not completed: %s", run.Status)
	}
	f := findingOf(t, h, run.ID, store.FindingDeleted)
	if f.Status != store.FindingApplied {
		t.Fatalf("deleted finding not applied: %s", f.Status)
	}

	// The provider must no longer serve the record as live (soft-deleted).
	waitFor(t, 10*time.Second, "provider record deleted", func() bool {
		for _, r := range h.sfCustomers(t) {
			if fmt.Sprint(r["id"]) == fmt.Sprint(ids["sf_id"]) {
				return r["deleted"] == true
			}
		}
		return true
	})
}

// TestReconcileManualParksThenOperatorApplies proves a manual-mode run parks
// findings without mutating anything, and the operator's apply through the API
// repairs the divergence durably (via the retry queue).
func TestReconcileManualParksThenOperatorApplies(t *testing.T) {
	h := newPipelineHarness(t)
	ids := createSFAndPropagate(t, h, "sf-000003", "Manual", "Recon", "manual.recon@example.com")

	// Out-of-band drift again.
	h.sfSrv.Store.Update(fmt.Sprint(ids["sf_id"]), map[string]any{"last_name": "Drifted"})

	run := startReconcile(t, h, "salesforce", "manual")
	if run.Status != store.ReconcileComplete {
		t.Fatalf("run not completed: %s", run.Status)
	}
	f := findingOf(t, h, run.ID, store.FindingDrift)
	if f.Status != store.FindingPending {
		t.Fatalf("manual finding should be parked as pending, got %s", f.Status)
	}

	// Nothing was repaired by the run.
	if last := fmt.Sprint(sfCustomerByEmail(t, h, "manual.recon@example.com")["last_name"]); last != "Drifted" {
		t.Fatalf("manual run must not mutate provider; last_name=%q", last)
	}

	// Start the retry engine and apply the finding through the API.
	h.startRetry()
	applyBody := bytes.NewBufferString(`{"direction":"push_canonical"}`)
	areq, err := http.NewRequest(http.MethodPost,
		h.api.URL+"/api/v1/reconciliations/"+run.ID+"/findings/"+f.ID+"/apply", applyBody)
	if err != nil {
		t.Fatal(err)
	}
	areq.Header.Set("Content-Type", "application/json")
	areq.Header.Set("X-API-Key", "sfk_acme_dev")
	resp, err := http.DefaultClient.Do(areq)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("apply finding status %d", resp.StatusCode)
	}

	// The durable retry queue applies the repair and transitions the finding.
	h.drainRetries(t, 15*time.Second, "operator apply repaired drift", func() bool {
		got, err := store.GetReconciliationFinding(context.Background(), h.db.App, h.acmeID, f.ID)
		return err == nil && got.Status == store.FindingApplied
	})

	if last := fmt.Sprint(sfCustomerByEmail(t, h, "manual.recon@example.com")["last_name"]); last != "Recon" {
		t.Fatalf("operator apply must repair provider; last_name=%q", last)
	}
}

// TestReconcileAutoRecreatesMissingUnderIgnorePolicy proves a record that
// vanished from the provider (out-of-band delete) is re-created when the
// tenant's delete policy treats external deletions as ignorable.
func TestReconcileAutoRecreatesMissingUnderIgnorePolicy(t *testing.T) {
	h := newPipelineHarness(t)
	ids := createSFAndPropagate(t, h, "sf-000004", "Ghost", "Recon", "ghost.recon@example.com")

	// Soft-delete the provider record directly (bypassing webhooks): the
	// provider no longer serves it live, but canonical + hubspot still have it.
	h.sfSrv.Store.SoftDelete(fmt.Sprint(ids["sf_id"]))

	// The Acme policy defaults to propagate, which respects external deletes:
	// an auto run must park (skip) the missing finding, not resurrect it.
	run := startReconcile(t, h, "salesforce", "auto")
	f := findingOf(t, h, run.ID, store.FindingMissing)
	if f.Status != store.FindingSkipped {
		t.Fatalf("missing finding under propagate should be skipped, got %s", f.Status)
	}

	// Now switch the policy to ignore and re-run: the record is re-created.
	for _, p := range []store.SyncPolicy{
		{TenantID: h.acmeID, Entity: "customer", Source: "salesforce", Destination: "hubspot", Mode: "bidirectional", ConflictStrategy: "field_merge", DeletePolicy: "ignore", RetryPolicy: "exponential_backoff", SourcePriority: 100, Enabled: true},
		{TenantID: h.acmeID, Entity: "customer", Source: "hubspot", Destination: "salesforce", Mode: "bidirectional", ConflictStrategy: "field_merge", DeletePolicy: "ignore", RetryPolicy: "exponential_backoff", SourcePriority: 200, Enabled: true},
	} {
		if _, err := store.UpsertSyncPolicy(context.Background(), h.db.App, p); err != nil {
			t.Fatalf("upsert ignore policy: %v", err)
		}
	}

	run2 := startReconcile(t, h, "salesforce", "auto")
	waitFor(t, 10*time.Second, "provider record re-created", func() bool {
		for _, r := range h.sfCustomers(t) {
			if r["email"] == "ghost.recon@example.com" {
				return true
			}
		}
		return false
	})
	// A new (or same) live record must exist and the finding is applied.
	if got := findingOf(t, h, run2.ID, store.FindingMissing).Status; got != store.FindingApplied {
		t.Fatalf("missing finding should be applied under ignore policy, got %s", got)
	}
}

// TestReconcileMissedAdoptsUnknownProviderRecord proves a provider record that
// SyncForge never ingested (missed) is adopted into the canonical model and
// propagated to the destination by an auto run.
func TestReconcileMissedAdoptsUnknownProviderRecord(t *testing.T) {
	h := newPipelineHarness(t)

	// Disable the salesforce webhook so a direct provider write is NOT ingested
	// by SyncForge: SyncForge has never seen this record, so a reconcile run
	// must classify it as missed and adopt it.
	h.sfSrv.SetWebhook("", "")

	body, err := json.Marshal(map[string]any{
		"first_name": "Mystery", "last_name": "Record",
		"email": "mystery.recon@example.com", "phone": "+1-555-7777", "company": "Unknown",
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(h.sfSim.URL+"/api/v1/customers", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create sf record status %d", resp.StatusCode)
	}

	run := startReconcile(t, h, "salesforce", "auto")
	if run.Missed != 1 {
		t.Fatalf("expected 1 missed finding, got %d", run.Missed)
	}
	f := findingOf(t, h, run.ID, store.FindingMissed)
	if f.Status != store.FindingApplied {
		t.Fatalf("missed finding not applied: %s", f.Status)
	}

	// The adopted record must now exist on the destination.
	waitFor(t, 10*time.Second, "adopted record propagated to hubspot", func() bool {
		return findHub(h.hubContacts(t), func(r map[string]any) bool {
			return r["emailAddress"] == "mystery.recon@example.com"
		}) != nil
	})
}
