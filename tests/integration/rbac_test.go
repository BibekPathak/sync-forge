//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"syncforge/internal/api"
	"syncforge/internal/store"
)

// keyReq builds an authenticated request against the SyncForge API using a
// specific API key value.
func keyReq(t *testing.T, method, path, apiKey string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-API-Key", apiKey)
	return req
}

// postKeyReq sends a JSON body authenticated with the given key.
func postKeyReq(t *testing.T, path, apiKey string, body any) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(http.MethodPost, path, &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-API-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// TestRBACKeyLifecycle proves the API key management surface: an ADMIN mints
// keys (raw shown once), lists them, and can revoke them so they stop
// authenticating. A key may not revoke itself.
func TestRBACKeyLifecycle(t *testing.T) {
	ts, _ := newAPIServer(t)
	admin := "sfk_acme_dev"

	// Mint a VIEWER key; the raw credential comes back exactly once.
	resp := postKeyReq(t, ts.URL+"/api/v1/keys", admin, map[string]any{"name": "viewer-bot", "role": "VIEWER"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 creating key, got %d", resp.StatusCode)
	}
	var created struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Role   string `json:"role"`
		APIKey string `json:"api_key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if created.APIKey == "" {
		t.Fatal("expected raw api_key in create response")
	}
	if created.Role != "VIEWER" {
		t.Fatalf("expected VIEWER role, got %s", created.Role)
	}

	// The new key authenticates.
	req := keyReq(t, http.MethodGet, ts.URL+"/api/v1/connections", created.APIKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with minted key, got %d", resp.StatusCode)
	}

	// A non-admin cannot mint an ADMIN key.
	viewerKey := created.APIKey
	resp = postKeyReq(t, ts.URL+"/api/v1/keys", viewerKey, map[string]any{"name": "escalation", "role": "ADMIN"})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 minting above role, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// List shows the key (without any raw value).
	req = keyReq(t, http.MethodGet, ts.URL+"/api/v1/keys", admin)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var listed struct {
		Keys []store.APIKey `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listed); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(listed.Keys) < 2 {
		t.Fatalf("expected at least 2 keys (seed + minted), got %d", len(listed.Keys))
	}

	// Revoke it.
	req = keyReq(t, http.MethodPost, ts.URL+"/api/v1/keys/"+created.ID+"/revoke", admin)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 revoking key, got %d", resp.StatusCode)
	}

	// Revoked key no longer authenticates.
	req = keyReq(t, http.MethodGet, ts.URL+"/api/v1/connections", created.APIKey)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 after revocation, got %d", resp.StatusCode)
	}

	// A key cannot revoke itself.
	req = keyReq(t, http.MethodPost, ts.URL+"/api/v1/keys/"+created.ID+"/revoke", admin)
	// use the admin key's own id: look it up first via list to avoid coupling
	// to seed ids; revoke self must be rejected.
	req = keyReq(t, http.MethodGet, ts.URL+"/api/v1/keys", admin)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(resp.Body).Decode(&listed); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	var adminKeyID string
	for _, k := range listed.Keys {
		if k.Role == "ADMIN" {
			adminKeyID = k.ID
			break
		}
	}
	if adminKeyID == "" {
		t.Fatal("expected to find an admin key id")
	}
	req = keyReq(t, http.MethodPost, ts.URL+"/api/v1/keys/"+adminKeyID+"/revoke", admin)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 revoking self, got %d", resp.StatusCode)
	}
}

// TestRBACRoleEnforcement proves the endpoint role gates: VIEWER reads but
// cannot mutate, DEVELOPER can configure connections but not resolve conflicts,
// OPERATOR can resolve, and unknown roles fail closed.
func TestRBACRoleEnforcement(t *testing.T) {
	ts, srv := newAPIServer(t)
	admin := "sfk_acme_dev"

	mint := func(name, role string) string {
		t.Helper()
		resp := postKeyReq(t, ts.URL+"/api/v1/keys", admin, map[string]any{"name": name, "role": role})
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("mint %s %s: expected 201, got %d", name, role, resp.StatusCode)
		}
		var out struct {
			APIKey string `json:"api_key"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return out.APIKey
	}
	viewer := mint("rbac-viewer", "VIEWER")
	developer := mint("rbac-developer", "DEVELOPER")
	operator := mint("rbac-operator", "OPERATOR")

	// VIEWER can read connections.
	resp, err := http.DefaultClient.Do(keyReq(t, http.MethodGet, ts.URL+"/api/v1/connections", viewer))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("viewer read: expected 200, got %d", resp.StatusCode)
	}

	// VIEWER cannot create a connection.
	resp = postKeyReq(t, ts.URL+"/api/v1/connections", viewer, map[string]any{
		"name": "nope", "provider": "salesforce", "base_url": "http://x", "webhook_secret": "x",
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("viewer write: expected 403, got %d", resp.StatusCode)
	}

	// DEVELOPER can create a connection.
	resp = postKeyReq(t, ts.URL+"/api/v1/connections", developer, map[string]any{
		"name": "dev-write", "provider": "salesforce", "base_url": "http://dev", "webhook_secret": "s",
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("developer write: expected 201, got %d", resp.StatusCode)
	}

	// DEVELOPER cannot resolve conflicts (OPERATOR+ only).
	tenantID := tenantIDFor(t, srv, "acme")
	seedConflict(t, srv, tenantID)
	var cid string
	conflicts, err := store.ListConflicts(context.Background(), srv.DB().App, tenantID, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) == 0 {
		t.Fatal("expected a seeded conflict")
	}
	cid = conflicts[0].ID
	resp = postKeyReq(t, ts.URL+"/api/v1/conflicts/"+cid+"/resolve", developer, map[string]any{"side": "a"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("developer resolve: expected 403, got %d", resp.StatusCode)
	}

	// OPERATOR can resolve the conflict.
	resp = postKeyReq(t, ts.URL+"/api/v1/conflicts/"+cid+"/resolve", operator, map[string]any{"side": "a"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("operator resolve: expected 202, got %d", resp.StatusCode)
	}

	// VIEWER cannot trigger a reconciliation run.
	resp = postKeyReq(t, ts.URL+"/api/v1/reconciliations", viewer, map[string]any{"entity": "customer", "source": "salesforce", "mode": "manual"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("viewer reconcile: expected 403, got %d", resp.StatusCode)
	}

	// A non-existent role cannot be minted at all.
	resp = postKeyReq(t, ts.URL+"/api/v1/keys", admin, map[string]any{"name": "ghost", "role": "GOD"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown role: expected 400, got %d", resp.StatusCode)
	}
}

// seedConflict inserts a pending conflict row for the tenant so role gates on
// the resolve endpoint can be exercised without driving the full pipeline.
func seedConflict(t *testing.T, srv *api.Server, tenantID string) {
	t.Helper()
	payload := []byte(`{"fields":{"last_name":"x"}}`)
	_, err := store.InsertConflict(context.Background(), srv.DB().App, store.ConflictRecord{
		TenantID:           tenantID,
		EntityType:         "customer",
		EntityID:           "sf-000099",
		SourceA:            "salesforce",
		VersionA:           1,
		PayloadA:           payload,
		SourceB:            "hubspot",
		VersionB:           1,
		PayloadB:           payload,
		Status:             store.ConflictPending,
		ResolutionStrategy: "manual",
	})
	if err != nil {
		t.Fatalf("seed conflict: %v", err)
	}
}
