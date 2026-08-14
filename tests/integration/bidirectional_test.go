//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"syncforge/internal/store"
)

// mutateSF patches a Salesforce simulator record and waits for the pipeline.
func (h *pipelineHarness) mutateSF(t *testing.T, id string, fields map[string]any) map[string]any {
	t.Helper()
	body, _ := json.Marshal(fields)
	req, _ := http.NewRequest(http.MethodPatch, h.sfSim.URL+"/api/v1/customers/"+id, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sf patch status %d", resp.StatusCode)
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}

// createSF creates a customer in the Salesforce simulator; the resulting
// webhook flows through the pipeline.
func (h *pipelineHarness) createSF(t *testing.T, fields map[string]any) map[string]any {
	t.Helper()
	body, _ := json.Marshal(fields)
	resp, err := http.Post(h.sfSim.URL+"/api/v1/customers", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("sf create status %d", resp.StatusCode)
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}

// mutateHub patches a HubSpot simulator contact and waits for the pipeline.
func (h *pipelineHarness) mutateHub(t *testing.T, id string, fields map[string]any) map[string]any {
	t.Helper()
	body, _ := json.Marshal(fields)
	req, _ := http.NewRequest(http.MethodPatch, h.hubSim.URL+"/api/v1/contacts/"+id, bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("hub patch status %d", resp.StatusCode)
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}

func (h *pipelineHarness) createHub(t *testing.T, fields map[string]any) map[string]any {
	t.Helper()
	body, _ := json.Marshal(fields)
	resp, err := http.Post(h.hubSim.URL+"/api/v1/contacts", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("hub create status %d", resp.StatusCode)
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}

// canonical returns the single canonical record for an entity id.
func (h *pipelineHarness) canonical(t *testing.T, entityID string) store.CanonicalRecord {
	t.Helper()
	c, err := store.GetCanonical(context.Background(), h.db.App, h.acmeID, "customer", entityID)
	if err != nil {
		t.Fatalf("get canonical %s: %v", entityID, err)
	}
	return c
}

// TestBidirectionalSync proves both directions propagate and settle: SF->HS,
// then a genuine HS change -> SF, with no runaway loops.
func TestBidirectionalSync(t *testing.T) {
	h := newPipelineHarness(t)

	// Seed a customer in Salesforce; it should appear in HubSpot.
	created := h.createSF(t, map[string]any{
		"first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com",
		"phone": "+1-555-1000", "company": "Analytical",
	})
	sfID := created["id"].(string)

	var hub map[string]any
	waitFor(t, 10*time.Second, "hubspot contact created", func() bool {
		hub = findHub(h.hubContacts(t), func(r map[string]any) bool {
			return r["emailAddress"] == "ada@example.com"
		})
		return hub != nil
	})
	if hub["contact_id"] == "" {
		t.Fatal("hubspot contact missing id")
	}

	// Genuine change in HubSpot (different field) -> Salesforce must receive it.
	h.mutateHub(t, hub["contact_id"].(string), map[string]any{"phoneNumber": "+1-999-0000"})
	waitFor(t, 10*time.Second, "salesforce receives hubspot change", func() bool {
		recs := h.sfCustomers(t)
		for _, r := range recs {
			if r["id"] == sfID && r["phone"] == "+1-999-0000" {
				return true
			}
		}
		return false
	})

	// Canonical must know both provider ids.
	canonical := h.canonical(t, sfID)
	if canonical.ProviderIDs["salesforce"] == "" || canonical.ProviderIDs["hubspot"] == "" {
		t.Fatalf("bidirectional mapping incomplete: %+v", canonical.ProviderIDs)
	}

	// Allow echoes to propagate; the system must settle (no oscillation).
	time.Sleep(2 * time.Second)
	canonV1 := h.canonical(t, sfID).Version
	hubV1 := versionOf(t, h.hubContacts(t), hub["contact_id"].(string))
	sfV1 := versionOf(t, h.sfCustomers(t), sfID)
	time.Sleep(2 * time.Second)
	if got := h.canonical(t, sfID).Version; got != canonV1 {
		t.Fatalf("canonical version kept changing (loop?): %d -> %d", canonV1, got)
	}
	if got := versionOf(t, h.hubContacts(t), hub["contact_id"].(string)); got != hubV1 {
		t.Fatalf("hubspot version kept changing (loop?): %d -> %d", hubV1, got)
	}
	if got := versionOf(t, h.sfCustomers(t), sfID); got != sfV1 {
		t.Fatalf("salesforce version kept changing (loop?): %d -> %d", sfV1, got)
	}
}

// TestLoopPrevention proves that a change propagated to HubSpot, when echoed
// back by HubSpot's webhook, is recognized as SyncForge's own write and does
// NOT bounce back to Salesforce.
func TestLoopPrevention(t *testing.T) {
	h := newPipelineHarness(t)

	// Create the customer in SF.
	created := h.createSF(t, map[string]any{
		"first_name": "Loop", "last_name": "One", "email": "loop@example.com",
		"phone": "+1-555-2000", "company": "L",
	})
	sfID := created["id"].(string)
	var hub map[string]any
	waitFor(t, 10*time.Second, "hubspot contact created", func() bool {
		hub = findHub(h.hubContacts(t), func(r map[string]any) bool {
			return r["emailAddress"] == "loop@example.com"
		})
		return hub != nil
	})

	// Now a real SF change. It propagates once to HubSpot.
	h.mutateSF(t, sfID, map[string]any{"email": "loop-2@example.com"})
	waitFor(t, 10*time.Second, "hubspot receives sf change", func() bool {
		c := h.hubContacts(t)
		for _, r := range c {
			if r["contact_id"] == hub["contact_id"] && r["emailAddress"] == "loop-2@example.com" {
				return true
			}
		}
		return false
	})

	// Wait long enough for HubSpot's echo webhook to be ingested and processed.
	time.Sleep(3 * time.Second)

	// Salesforce must NOT have been re-written by the echo (its email is the
	// value we set; a loop would have bounced it back and re-incremented).
	sf := findHub(h.sfCustomers(t), func(r map[string]any) bool { return r["id"] == sfID })
	if sf["email"] != "loop-2@example.com" {
		t.Fatalf("unexpected salesforce email after echo: %v", sf["email"])
	}

	// And the canonical must be stable.
	canonV1 := h.canonical(t, sfID).Version
	time.Sleep(2 * time.Second)
	if got := h.canonical(t, sfID).Version; got != canonV1 {
		t.Fatalf("canonical version kept changing after echo (loop): %d -> %d", canonV1, got)
	}
}

// TestOutOfOrderEvents proves events arriving out of order are dropped: after
// v3 is applied, a late v2 must not overwrite the destination.
func TestOutOfOrderEvents(t *testing.T) {
	h := newPipelineHarness(t)

	// v3 arrives first.
	webhookV3 := `{
		"event_id": "ooo-v3",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000003",
		"event_type": "updated",
		"source_version": 3,
		"occurred_at": "2024-01-01T00:03:00Z",
		"payload": {"fields": {"id": "sf-000003", "first_name": "Three", "last_name": "B",
			"email": "v3@example.com", "phone": "+1-3", "company": "C"}}
	}`
	resp, _ := postWebhook(t, h.api, "sfs-dev-secret", webhookV3)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("v3 webhook status %d", resp.StatusCode)
	}
	waitFor(t, 10*time.Second, "v3 applied", func() bool {
		return findHub(h.hubContacts(t), func(r map[string]any) bool {
			return r["emailAddress"] == "v3@example.com"
		}) != nil
	})

	// v2 arrives late.
	webhookV2 := `{
		"event_id": "ooo-v2",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000003",
		"event_type": "updated",
		"source_version": 2,
		"occurred_at": "2024-01-01T00:02:00Z",
		"payload": {"fields": {"id": "sf-000003", "first_name": "Two", "last_name": "B",
			"email": "v2@example.com", "phone": "+1-2", "company": "C"}}
	}`
	resp, _ = postWebhook(t, h.api, "sfs-dev-secret", webhookV2)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("v2 webhook status %d", resp.StatusCode)
	}

	// v2 must be dropped: destination still shows v3 state, source version 3.
	time.Sleep(2 * time.Second)
	for _, r := range h.hubContacts(t) {
		if r["emailAddress"] == "v2@example.com" {
			t.Fatal("stale v2 overwrote the destination")
		}
	}
	canonical := h.canonical(t, "sf-000003")
	if canonical.SourceVersions["salesforce"] != 3 {
		t.Fatalf("expected source version 3 after out-of-order handling, got %d", canonical.SourceVersions["salesforce"])
	}
}

// TestIdentityResolutionByEmail proves an independently created HubSpot contact
// with a matching email links to the existing canonical entity instead of
// creating a duplicate.
func TestIdentityResolutionByEmail(t *testing.T) {
	h := newPipelineHarness(t)

	// SF customer with email X.
	created := h.createSF(t, map[string]any{
		"first_name": "Ada", "last_name": "Lovelace", "email": "shared@example.com",
		"phone": "+1-555-3000", "company": "A",
	})
	sfID := created["id"].(string)
	waitFor(t, 10*time.Second, "hubspot contact created", func() bool {
		return findHub(h.hubContacts(t), func(r map[string]any) bool {
			return r["emailAddress"] == "shared@example.com"
		}) != nil
	})

	// A brand-new HubSpot contact with the same email (different id, created
	// independently in HubSpot).
	newHub := h.createHub(t, map[string]any{
		"firstName": "Grace", "lastName": "Hopper", "emailAddress": "shared@example.com",
		"phoneNumber": "+1-777", "organization": "Navy",
	})

	// The pipeline must resolve it to the existing canonical by email.
	waitFor(t, 10*time.Second, "identity resolved and propagated to salesforce", func() bool {
		recs := h.sfCustomers(t)
		for _, r := range recs {
			if r["id"] == sfID && r["first_name"] == "Grace" {
				return true
			}
		}
		return false
	})

	// Exactly one canonical record owns both provider ids.
	canonical := h.canonical(t, sfID)
	if canonical.ProviderIDs["salesforce"] != sfID {
		t.Fatalf("salesforce id mapping wrong: %+v", canonical.ProviderIDs)
	}
	if canonical.ProviderIDs["hubspot"] != newHub["contact_id"] {
		t.Fatalf("hubspot id should be linked to new contact: %+v", canonical.ProviderIDs)
	}
}

// hubContactIDForSF returns the hubspot contact id linked to an SF customer.
func (h *pipelineHarness) hubContactIDForSF(t *testing.T, sfID string) string {
	t.Helper()
	c := h.canonical(t, sfID)
	return c.ProviderIDs["hubspot"]
}

func versionOf(t *testing.T, recs []map[string]any, id string) int64 {
	t.Helper()
	for _, r := range recs {
		if r["id"] == id || r["contact_id"] == id {
			switch v := r["version"].(type) {
			case float64:
				return int64(v)
			case int:
				return int64(v)
			}
		}
	}
	t.Fatalf("record %s not found", id)
	return 0
}
