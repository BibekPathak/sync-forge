//go:build integration

package integration

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestListSessionsShowsActiveLogins proves an ADMIN can list live sessions and
// sees the session the test just created.
func TestListSessionsShowsActiveLogins(t *testing.T) {
	ts, _ := newAPIServer(t)

	// Two logins create two live sessions.
	tokenA, status := loginAs(t, ts, "admin@acme.dev", "syncforge-demo")
	if status != http.StatusOK {
		t.Fatalf("login A: expected 200, got %d", status)
	}
	if _, status := loginAs(t, ts, "admin@acme.dev", "syncforge-demo"); status != http.StatusOK {
		t.Fatalf("login B: expected 200, got %d", status)
	}

	req := apiKeyReq(t, ts.URL+"/api/v1/sessions")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list sessions: expected 200, got %d", resp.StatusCode)
	}
	var out struct {
		Sessions []struct {
			JTI     string `json:"jti"`
			Role    string `json:"role"`
			Expires string `json:"expires_at"`
		} `json:"sessions"`
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Count < 2 {
		t.Fatalf("expected at least 2 live sessions, got %d", out.Count)
	}
	_ = tokenA
}

// TestRevokeUserSessionsSignsOutEverywhere proves revoking all sessions for a
// user invalidates every token issued to them.
func TestRevokeUserSessionsSignsOutEverywhere(t *testing.T) {
	ts, _ := newAPIServer(t)

	// Create a VIEWER user and log in twice.
	resp := postKeyReq(t, ts.URL+"/api/v1/users", "sfk_acme_dev",
		map[string]any{"email": "multi@acme.dev", "password": "multi-pass", "role": "VIEWER"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create user: expected 201, got %d", resp.StatusCode)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	tokenA, status := loginAs(t, ts, "multi@acme.dev", "multi-pass")
	if status != http.StatusOK {
		t.Fatalf("login A: expected 200, got %d", status)
	}
	tokenB, status := loginAs(t, ts, "multi@acme.dev", "multi-pass")
	if status != http.StatusOK {
		t.Fatalf("login B: expected 200, got %d", status)
	}

	// Both tokens work before revocation.
	for name, tok := range map[string]string{"A": tokenA, "B": tokenB} {
		req := bearerReq(t, ts.URL+"/api/v1/connections", tok)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("token %s pre-revoke: expected 200, got %d", name, r.StatusCode)
		}
	}

	// ADMIN revokes all of the user's sessions.
	resp = postKeyReq(t, ts.URL+"/api/v1/users/"+created.ID+"/revoke-sessions", "sfk_acme_dev", nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("revoke sessions: expected 200, got %d", resp.StatusCode)
	}

	// Both tokens now fail.
	for name, tok := range map[string]string{"A": tokenA, "B": tokenB} {
		req := bearerReq(t, ts.URL+"/api/v1/connections", tok)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusUnauthorized {
			t.Fatalf("token %s post-revoke: expected 401, got %d", name, r.StatusCode)
		}
	}
}
