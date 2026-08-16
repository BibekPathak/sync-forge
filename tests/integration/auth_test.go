//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// loginAs authenticates as the seeded demo user and returns the session token.
func loginAs(t *testing.T, ts *httptest.Server, email, password string) (string, int) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"tenant_slug": "acme",
		"email":       email,
		"password":    password,
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(ts.URL+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.Token, resp.StatusCode
}

// TestUserLoginAndSessionToken proves the seeded demo user can log in and the
// resulting session token authenticates against role-gated endpoints.
func TestUserLoginAndSessionToken(t *testing.T) {
	ts, _ := newAPIServer(t)

	token, status := loginAs(t, ts, "admin@acme.dev", "syncforge-demo")
	if status != http.StatusOK || token == "" {
		t.Fatalf("expected 200 + token, got status=%d token=%q", status, token)
	}

	// The session token reads tenant-scoped resources (ADMIN role).
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/connections", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 reading connections with user token, got %d", resp.StatusCode)
	}

	// The token is ADMIN, so it can mint keys / list users.
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/api/v1/users", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 listing users with admin user token, got %d", resp.StatusCode)
	}
}

// TestUserLoginRejectsBadCredentials proves login fails closed on unknown
// tenant, unknown email, or wrong password.
func TestUserLoginRejectsBadCredentials(t *testing.T) {
	ts, _ := newAPIServer(t)

	cases := []map[string]any{
		{"tenant_slug": "acme", "email": "admin@acme.dev", "password": "wrong-password"},
		{"tenant_slug": "acme", "email": "nobody@acme.dev", "password": "syncforge-demo"},
		{"tenant_slug": "nope", "email": "admin@acme.dev", "password": "syncforge-demo"},
	}
	for _, c := range cases {
		body, _ := json.Marshal(c)
		resp, err := http.Post(ts.URL+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401 for %v, got %d", c, resp.StatusCode)
		}
	}
}

// TestUserSessionTokenForbidsHigherRole proves a VIEWER user token cannot reach
// ADMIN-only endpoints.
func TestUserSessionTokenForbidsHigherRole(t *testing.T) {
	ts, _ := newAPIServer(t)

	// ADMIN (API key) creates a VIEWER user.
	body, _ := json.Marshal(map[string]any{
		"email": "viewer@acme.dev", "password": "viewer-pass", "role": "VIEWER",
	})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/users", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "sfk_acme_dev")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create user: expected 201, got %d", resp.StatusCode)
	}

	// The viewer logs in.
	viewerToken, status := loginAs(t, ts, "viewer@acme.dev", "viewer-pass")
	if status != http.StatusOK {
		t.Fatalf("viewer login: expected 200, got %d", status)
	}

	// VIEWER can read connections.
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/api/v1/connections", nil)
	req.Header.Set("Authorization", "Bearer "+viewerToken)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("viewer read: expected 200, got %d", resp.StatusCode)
	}

	// VIEWER cannot list users (ADMIN only).
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/api/v1/users", nil)
	req.Header.Set("Authorization", "Bearer "+viewerToken)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("viewer admin access: expected 403, got %d", resp.StatusCode)
	}

	// VIEWER cannot create users at all.
	body, _ = json.Marshal(map[string]any{
		"email": "another@acme.dev", "password": "x", "role": "VIEWER",
	})
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/api/v1/users", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+viewerToken)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("viewer create user: expected 403, got %d", resp.StatusCode)
	}
}
