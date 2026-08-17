//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"syncforge/internal/totp"
)

// postJSONAuthenticated sends a JSON POST with the given auth header value.
func postJSONAuthenticated(t *testing.T, ts *httptest.Server, path, authHeader, authValue string, body any) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if authHeader != "" {
		req.Header.Set(authHeader, authValue)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// TestMFALifecycle proves the full MFA flow: a user logs in, enrolls a TOTP
// secret, confirms it with a code (enabling MFA), then login requires a valid
// code — a password-only login is rejected and a bad code is rejected.
func TestMFALifecycle(t *testing.T) {
	ts, _ := newAPIServer(t)

	// Login (MFA not yet enabled) to get a session for self-service enroll.
	token, status := loginAs(t, ts, "admin@acme.dev", "syncforge-demo")
	if status != http.StatusOK {
		t.Fatalf("initial login: expected 200, got %d", status)
	}

	// Enroll: returns a secret + otpauth URI.
	resp := postJSONAuthenticated(t, ts, "/api/v1/auth/mfa/enroll", "Authorization", "Bearer "+token, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("enroll: expected 200, got %d", resp.StatusCode)
	}
	var enroll struct {
		Secret  string `json:"secret"`
		OtpAuth string `json:"otpauth"`
		Enabled bool   `json:"enabled"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&enroll); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if enroll.Secret == "" || enroll.OtpAuth == "" {
		t.Fatalf("enroll missing secret/uri: %+v", enroll)
	}
	if enroll.Enabled {
		t.Fatal("enroll must leave mfa disabled until confirmed")
	}

	// Confirm with the current code.
	code, err := totp.Code(enroll.Secret, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	resp = postJSONAuthenticated(t, ts, "/api/v1/auth/mfa/confirm", "Authorization", "Bearer "+token, map[string]any{"code": code})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("confirm: expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Now password-only login fails: MFA code required.
	if _, status := loginAs(t, ts, "admin@acme.dev", "syncforge-demo"); status != http.StatusUnauthorized {
		t.Fatalf("password-only login after mfa: expected 401, got %d", status)
	}

	// A wrong code fails.
	badCode, _ := totp.Code(enroll.Secret, time.Now().Add(-5*time.Minute).UTC())
	if _, status := loginAsWithCode(t, ts, "admin@acme.dev", "syncforge-demo", badCode); status != http.StatusUnauthorized {
		t.Fatalf("login with bad mfa code: expected 401, got %d", status)
	}

	// A valid code succeeds.
	if _, status := loginAsWithCode(t, ts, "admin@acme.dev", "syncforge-demo", code); status != http.StatusOK {
		t.Fatalf("login with valid mfa code: expected 200, got %d", status)
	}
}

// loginAsWithCode is like loginAs but includes the TOTP code.
func loginAsWithCode(t *testing.T, ts *httptest.Server, email, password, code string) (string, int) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"tenant_slug": "acme",
		"email":       email,
		"password":    password,
		"code":        code,
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
