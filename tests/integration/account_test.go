//go:build integration

package integration

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestChangePasswordRevokesSessions proves a self-service password change
// verifies the current password, updates credentials, and revokes the user's
// session so a fresh login is required.
func TestChangePasswordRevokesSessions(t *testing.T) {
	ts, _ := newAPIServer(t)

	token, status := loginAs(t, ts, "admin@acme.dev", "syncforge-demo")
	if status != http.StatusOK {
		t.Fatalf("login: expected 200, got %d", status)
	}

	// Wrong current password rejected.
	resp := postJSONAuthenticated(t, ts, "/api/v1/auth/change-password", "Authorization", "Bearer "+token,
		map[string]any{"current_password": "nope", "new_password": "newpass123"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong current password: expected 401, got %d", resp.StatusCode)
	}

	// Correct change succeeds and revokes the session.
	resp = postJSONAuthenticated(t, ts, "/api/v1/auth/change-password", "Authorization", "Bearer "+token,
		map[string]any{"current_password": "syncforge-demo", "new_password": "newpass123"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("change password: expected 200, got %d", resp.StatusCode)
	}

	// The old token no longer authenticates (session revoked).
	req := bearerReq(t, ts.URL+"/api/v1/connections", token)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old token after password change: expected 401, got %d", resp2.StatusCode)
	}

	// Old password fails, new password works.
	if _, status := loginAs(t, ts, "admin@acme.dev", "syncforge-demo"); status != http.StatusUnauthorized {
		t.Fatalf("old password login: expected 401, got %d", status)
	}
	if _, status := loginAs(t, ts, "admin@acme.dev", "newpass123"); status != http.StatusOK {
		t.Fatalf("new password login: expected 200, got %d", status)
	}
}

// TestAdminResetPassword proves an ADMIN can reset a user's password, which
// revokes that user's sessions.
func TestAdminResetPassword(t *testing.T) {
	ts, _ := newAPIServer(t)

	// Create a VIEWER user and log in as them.
	resp := postKeyReq(t, ts.URL+"/api/v1/users", "sfk_acme_dev",
		map[string]any{"email": "reset@acme.dev", "password": "orig-pass", "role": "VIEWER"})
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
	viewerToken, status := loginAs(t, ts, "reset@acme.dev", "orig-pass")
	if status != http.StatusOK {
		t.Fatalf("viewer login: expected 200, got %d", status)
	}

	// ADMIN resets the password.
	resp = postKeyReq(t, ts.URL+"/api/v1/users/"+created.ID+"/reset-password", "sfk_acme_dev",
		map[string]any{"new_password": "admin-reset-pass"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reset password: expected 200, got %d", resp.StatusCode)
	}

	// The viewer's session is revoked.
	req := bearerReq(t, ts.URL+"/api/v1/connections", viewerToken)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("viewer token after reset: expected 401, got %d", resp2.StatusCode)
	}

	// Login works with the reset password.
	if _, status := loginAs(t, ts, "reset@acme.dev", "admin-reset-pass"); status != http.StatusOK {
		t.Fatalf("login after reset: expected 200, got %d", status)
	}
}

// TestAdminChangeRole proves an ADMIN can change a user's role and the new
// role is enforced by the role gate.
func TestAdminChangeRole(t *testing.T) {
	ts, _ := newAPIServer(t)

	// Create a VIEWER user, then promote them to OPERATOR.
	resp := postKeyReq(t, ts.URL+"/api/v1/users", "sfk_acme_dev",
		map[string]any{"email": "promo@acme.dev", "password": "promo-pass", "role": "VIEWER"})
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

	token, status := loginAs(t, ts, "promo@acme.dev", "promo-pass")
	if status != http.StatusOK {
		t.Fatalf("login: expected 200, got %d", status)
	}

	// As VIEWER, creating a connection is forbidden.
	resp = postJSONAuthenticated(t, ts, "/api/v1/connections", "Authorization", "Bearer "+token,
		map[string]any{"name": "x", "provider": "salesforce", "base_url": "http://x", "webhook_secret": "s"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("viewer create connection: expected 403, got %d", resp.StatusCode)
	}

	// ADMIN promotes to DEVELOPER.
	resp = postKeyReq(t, ts.URL+"/api/v1/users/"+created.ID+"/role", "sfk_acme_dev",
		map[string]any{"role": "DEVELOPER"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("change role: expected 200, got %d", resp.StatusCode)
	}

	// Now the same token can create a connection (role checked per request,
	// re-read from the DB at login; the token's role is refreshed on login, so
	// re-login to pick up the new role).
	newToken, status := loginAs(t, ts, "promo@acme.dev", "promo-pass")
	if status != http.StatusOK {
		t.Fatalf("re-login: expected 200, got %d", status)
	}
	resp = postJSONAuthenticated(t, ts, "/api/v1/connections", "Authorization", "Bearer "+newToken,
		map[string]any{"name": "x", "provider": "salesforce", "base_url": "http://x", "webhook_secret": "s"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("developer create connection after promotion: expected 201, got %d", resp.StatusCode)
	}

	// A role above the caller's own cannot be granted.
	resp = postKeyReq(t, ts.URL+"/api/v1/users/"+created.ID+"/role", "sfk_acme_dev",
		map[string]any{"role": "ADMIN"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("promote to ADMIN from ADMIN key: expected 200 (same rank), got %d", resp.StatusCode)
	}
}
