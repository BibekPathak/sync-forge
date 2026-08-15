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

// overrideConflictStrategy re-upserts both Acme customer policies with a given
// conflict strategy, so a test can exercise manual vs auto resolution paths.
func overrideConflictStrategy(t *testing.T, h *pipelineHarness, strategy string) {
	t.Helper()
	for _, p := range []store.SyncPolicy{
		{TenantID: h.acmeID, Entity: "customer", Source: "salesforce", Destination: "hubspot", Mode: "bidirectional", DeletePolicy: "propagate", RetryPolicy: "exponential_backoff", SourcePriority: 100, Enabled: true},
		{TenantID: h.acmeID, Entity: "customer", Source: "hubspot", Destination: "salesforce", Mode: "bidirectional", DeletePolicy: "propagate", RetryPolicy: "exponential_backoff", SourcePriority: 200, Enabled: true},
	} {
		p.ConflictStrategy = strategy
		if _, err := store.UpsertSyncPolicy(context.Background(), h.db.App, p); err != nil {
			t.Fatalf("override policy %s->%s: %v", p.Source, p.Destination, err)
		}
	}
}

// createSFAndPropagate inserts a customer into the Salesforce simulator (which
// emits a signed webhook through the whole pipeline) and waits until the record
// exists in HubSpot and a canonical record was persisted.
func createSFAndPropagate(t *testing.T, h *pipelineHarness, id, first, last, email string) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"first_name": first, "last_name": last,
		"email": email, "phone": "+1-555-0100", "company": "Acme",
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
		t.Fatalf("create sf customer status %d", resp.StatusCode)
	}
	var created map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	// The simulator assigns the id; do not assume a specific prefix.
	id = fmt.Sprint(created["id"])

	deadline := time.Now().Add(10 * time.Second)
	for {
		if c, err := store.GetCanonicalByProvider(context.Background(), h.db.App, h.acmeID, "customer", "salesforce", id); err == nil && c.ProviderIDs["hubspot"] != "" {
			return map[string]any{
				"sf_id":        id,
				"hs_id":        c.ProviderIDs["hubspot"],
				"canonical_id": c.EntityID,
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("customer did not propagate to hubspot")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// patchHubspot edits a hubspot contact through the simulator, emitting an
// independently-authored update webhook (a concurrent edit on the HubSpot side).
func patchHubspot(t *testing.T, h *pipelineHarness, hsID string, data map[string]any) {
	t.Helper()
	body, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPatch, h.hubSim.URL+"/api/v1/contacts/"+hsID, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("patch hubspot contact status %d", resp.StatusCode)
	}
}

// conflictFor finds the single conflict for the tenant (optionally by status).
func conflictFor(t *testing.T, h *pipelineHarness, status string) store.ConflictRecord {
	t.Helper()
	items, err := store.ListConflicts(context.Background(), h.db.App, h.acmeID, status, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected exactly 1 conflict, got %d", len(items))
	}
	return items[0]
}

// sfCustomerByEmail returns the salesforce record matching email.
func sfCustomerByEmail(t *testing.T, h *pipelineHarness, email string) map[string]any {
	t.Helper()
	for _, r := range h.sfCustomers(t) {
		if r["email"] == email {
			return r
		}
	}
	t.Fatalf("salesforce customer %s not found", email)
	return nil
}

// TestManualConflictParksThenResolvesViaAPI proves the manual strategy: a
// concurrent HubSpot edit parks a CONFLICT_PENDING (no destination mutation,
// no auto-winner), the operator picks a side through the API, and the chosen
// side is durably applied to the destination without duplicates.
func TestManualConflictParksThenResolvesViaAPI(t *testing.T) {
	h := newPipelineHarness(t)
	overrideConflictStrategy(t, h, "manual")

	ids := createSFAndPropagate(t, h, "sf-000001", "Ada", "Lovelace", "ada@proxy.com")

	// Concurrent edit: HubSpot independently changes last_name AFTER salesforce
	// last wrote it. This is a real conflict, not an echo.
	patchHubspot(t, h, ids["hs_id"].(string), map[string]any{"lastName": "Turing"})

	// The conflict must be parked, and salesforce must be untouched.
	var conflict store.ConflictRecord
	waitFor(t, 10*time.Second, "conflict parked as pending", func() bool {
		items, err := store.ListConflicts(context.Background(), h.db.App, h.acmeID, "", 100)
		if err != nil {
			return false
		}
		for _, c := range items {
			if c.Status == store.ConflictPending {
				conflict = c
				return true
			}
		}
		return false
	})

	if conflict.SourceA != "salesforce" || conflict.SourceB != "hubspot" {
		t.Fatalf("unexpected conflict sides: %s vs %s", conflict.SourceA, conflict.SourceB)
	}
	if conflict.ResolutionStrategy != "manual" {
		t.Fatalf("expected manual strategy, got %s", conflict.ResolutionStrategy)
	}
	sf := sfCustomerByEmail(t, h, "ada@proxy.com")
	if last := fmt.Sprint(sf["last_name"]); last != "Lovelace" {
		t.Fatalf("manual conflict must not mutate destination; sf last_name=%q", last)
	}
	hs := findHub(h.hubContacts(t), func(r map[string]any) bool { return r["contact_id"] == ids["hs_id"] })
	if last := fmt.Sprint(hs["lastName"]); last != "Turing" {
		t.Fatalf("hubspot side should keep its edit; got %q", last)
	}

	// Resolve through the API, accepting the HubSpot side.
	resp2, err := http.DefaultClient.Do(mkResolveReq(t, h, conflict.ID, "b"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("resolve status %d", resp2.StatusCode)
	}

	// Drain the durable retry queue: the resolution event applies the winner.
	h.drainRetries(t, 15*time.Second, "manual resolution applied", func() bool {
		c, err := store.GetConflict(context.Background(), h.db.App, h.acmeID, conflict.ID)
		if err != nil {
			return false
		}
		return c.Status == store.ConflictResolved
	})

	// Salesforce now carries the hubspot-chosen value; single record each side.
	sf = sfCustomerByEmail(t, h, "ada@proxy.com")
	if last := fmt.Sprint(sf["last_name"]); last != "Turing" {
		t.Fatalf("resolved destination must carry winner; sf last_name=%q", last)
	}
	if n := len(h.hubContacts(t)); n != 1 {
		t.Fatalf("expected exactly 1 hubspot contact after resolution, got %d", n)
	}
	if n := len(h.sfCustomers(t)); n != 1 {
		t.Fatalf("expected exactly 1 salesforce customer, got %d", n)
	}
}

// mkResolveReq builds an authenticated POST body against the resolve endpoint.
func mkResolveReq(t *testing.T, h *pipelineHarness, id, side string) *http.Request {
	t.Helper()
	body := fmt.Sprintf(`{"side":%q}`, side)
	req := apiKeyReq(t, h.api.URL+"/api/v1/conflicts/"+id+"/resolve")
	req.Method = http.MethodPost
	req.Header.Set("Content-Type", "application/json")
	req.Body = http.NoBody
	// apiKeyReq sets method GET; construct the final request properly.
	rb, err := http.NewRequest(http.MethodPost, req.URL.String(), bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	rb.Header.Set("Content-Type", "application/json")
	rb.Header.Set("X-API-Key", "sfk_acme_dev")
	return rb
}

// TestFieldMergeAutoResolvesConcurrentEdit proves the field_merge strategy:
// the worker auto-resolves (winner by occurrence time), writes the merged value
// to the destination, and keeps an AUTO_RESOLVED audit row with both sides.
func TestFieldMergeAutoResolvesConcurrentEdit(t *testing.T) {
	h := newPipelineHarness(t)
	// Seeded policy is field_merge already; make it explicit.
	overrideConflictStrategy(t, h, "field_merge")

	ids := createSFAndPropagate(t, h, "sf-000002", "Grace", "Hopper", "grace@proxy.com")

	// Concurrent HubSpot edit, later in time -> it wins last_name.
	patchHubspot(t, h, ids["hs_id"].(string), map[string]any{"lastName": "Hopworth"})

	deadline := time.Now().Add(10 * time.Second)
	for {
		conflict, err := conflictForChecked(h)
		sfUpdated := false
		if err == nil && conflict.Status == store.ConflictAutoResolved {
			sf := sfCustomerByEmail(t, h, "grace@proxy.com")
			sfUpdated = fmt.Sprint(sf["last_name"]) == "Hopworth"
		}
		if sfUpdated {
			break
		}
		if time.Now().After(deadline) {
			c, cerr := conflictForChecked(h)
			t.Fatalf("field_merge did not auto-resolve: %v %v", c.Status, cerr)
		}
		time.Sleep(100 * time.Millisecond)
	}

	c := conflictFor(t, h, store.ConflictAutoResolved)
	if c.ResolutionStrategy != "field_merge" {
		t.Fatalf("expected field_merge strategy, got %s", c.ResolutionStrategy)
	}
	if c.SourceA != "salesforce" || c.SourceB != "hubspot" {
		t.Fatalf("unexpected conflict sides: %s vs %s", c.SourceA, c.SourceB)
	}

	// Winner (hubspot, later) must have propagated to salesforce exactly once.
	sf := sfCustomerByEmail(t, h, "grace@proxy.com")
	if last := fmt.Sprint(sf["last_name"]); last != "Hopworth" {
		t.Fatalf("expected merged winner, got sf last_name=%q", last)
	}
	if n := len(h.hubContacts(t)); n != 1 {
		t.Fatalf("expected exactly 1 hubspot contact, got %d", n)
	}

	// Field provenance must record the hubspot write on the merged field.
	canonical, err := store.GetCanonicalByProvider(context.Background(), h.db.App, h.acmeID, "customer", "salesforce", ids["sf_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	prov, ok := canonical.FieldProvenance["last_name"].(map[string]any)
	if !ok {
		t.Fatalf("field_provenance missing last_name: %+v", canonical.FieldProvenance)
	}
	if prov["source"] != "hubspot" {
		t.Fatalf("expected last_name writer hubspot, got %v", prov["source"])
	}
}

// conflictForChecked returns the single auto-resolved conflict (or error).
func conflictForChecked(h *pipelineHarness) (store.ConflictRecord, error) {
	items, err := store.ListConflicts(context.Background(), h.db.App, h.acmeID, store.ConflictAutoResolved, 10)
	if err != nil {
		return store.ConflictRecord{}, err
	}
	if len(items) == 0 {
		return store.ConflictRecord{}, fmt.Errorf("no auto-resolved conflict yet")
	}
	return items[0], nil
}
