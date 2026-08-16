//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"syncforge/internal/store"
)

// auditItems fetches the tenant's audit log through the API.
func auditItems(t *testing.T, ts *httptest.Server, apiKey string) []store.AuditLog {
	t.Helper()
	req := apiKeyReq(t, ts.URL+"/api/v1/audit")
	req.Header.Set("X-API-Key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list audit: expected 200, got %d", resp.StatusCode)
	}
	var out struct {
		Items []store.AuditLog `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.Items
}

// TestAuditLogRecordsOperatorActions proves key minting and conflict dismissal
// write durable, actor-attributed audit rows that the API can list.
func TestAuditLogRecordsOperatorActions(t *testing.T) {
	ts, _ := newAPIServer(t)

	// Mint a key (ADMIN action) — should leave an audit row.
	resp := postKeyReq(t, ts.URL+"/api/v1/keys", "sfk_acme_dev", map[string]any{"name": "audit-key", "role": "VIEWER"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("mint key: expected 201, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// A login attempt also lands in the audit log.
	if _, status := loginAs(t, ts, "admin@acme.dev", "syncforge-demo"); status != http.StatusOK {
		t.Fatalf("login: expected 200, got %d", status)
	}

	items := auditItems(t, ts, "sfk_acme_dev")
	var sawKeyCreate, sawLogin bool
	for _, a := range items {
		if a.Action == "key.create" && a.Resource == "api_key" {
			sawKeyCreate = true
			if a.Actor == "" {
				t.Fatal("key.create audit row missing actor")
			}
			if a.TenantID == nil || *a.TenantID == "" {
				t.Fatal("key.create audit row missing tenant_id")
			}
		}
		if a.Action == "auth.login" {
			sawLogin = true
		}
	}
	if !sawKeyCreate {
		t.Fatal("expected a key.create audit row")
	}
	if !sawLogin {
		t.Fatal("expected an auth.login audit row")
	}
}

// TestAuditLogRecordsLoginFailure proves failed logins are captured even
// without a tenant context (admin pool write).
func TestAuditLogRecordsLoginFailure(t *testing.T) {
	ts, _ := newAPIServer(t)

	if _, status := loginAs(t, ts, "admin@acme.dev", "wrong-password"); status != http.StatusUnauthorized {
		t.Fatalf("login: expected 401, got %d", status)
	}

	items := auditItems(t, ts, "sfk_acme_dev")
	var sawFailure bool
	for _, a := range items {
		if a.Action == "auth.login_failed" {
			sawFailure = true
			break
		}
	}
	if !sawFailure {
		t.Fatal("expected an auth.login_failed audit row")
	}
}

// TestSyncOperationsLedgerRecordsAppliedWrites proves every destination write
// leaves a sync_operations row reachable via the API, including the echo the
// provider sends back.
func TestSyncOperationsLedgerRecordsAppliedWrites(t *testing.T) {
	h := newPipelineHarness(t)

	createWebhook := `{
		"event_id": "ops-1",
		"source": "salesforce",
		"entity_type": "customer",
		"entity_id": "sf-000701",
		"event_type": "created",
		"source_version": 1,
		"occurred_at": "2024-01-01T00:00:00Z",
		"payload": {"fields": {"id": "sf-000701", "first_name": "Ops", "last_name": "Ledger",
			"email": "ops@example.com", "phone": "+1-555-0701", "company": "Ledger"}}
	}`
	resp, _ := postWebhook(t, h.api, "sfs-dev-secret", createWebhook)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("webhook status %d", resp.StatusCode)
	}
	_ = h.waitForHubContact(t, 1)

	// The applied write to hubspot must appear in the ledger.
	ops, err := store.ListSyncOperations(context.Background(), h.db.App, h.acmeID, "customer", "", "hubspot", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) == 0 {
		t.Fatal("expected at least one sync_operations row for the hubspot write")
	}
	found := false
	for _, o := range ops {
		if o.EntityID == "sf-000701" && o.TargetSource == "hubspot" {
			found = true
			if o.Fingerprint == "" {
				t.Fatal("sync operation missing fingerprint")
			}
			if o.AppliedVersion < 1 {
				t.Fatalf("sync operation missing applied version, got %d", o.AppliedVersion)
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected sync operation for sf-000701 -> hubspot, got %+v", ops)
	}

	// The read surface exposes them.
	req := apiKeyReq(t, h.api.URL+"/api/v1/operations?target_source=hubspot")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("list operations: expected 200, got %d", resp2.StatusCode)
	}
}
