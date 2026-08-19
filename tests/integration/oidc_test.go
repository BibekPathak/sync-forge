//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"

	"syncforge/internal/api"
	"syncforge/internal/cache"
	"syncforge/internal/config"
	"syncforge/internal/observability"
	"syncforge/internal/simulator"
)

// newOIDCAPIServer builds an API server configured to trust a mock OIDC
// provider and auto-provision SSO users. Returns the SyncForge API server, its
// httptest URL, and the mock IdP URL.
func newOIDCAPIServer(t *testing.T) (*httptest.Server, *api.Server, string) {
	t.Helper()
	database := newDB(t)

	idp, err := simulator.NewOIDCProvider("http://oidc.local", "syncforge-cli")
	if err != nil {
		t.Fatal(err)
	}
	idp.AddUser(simulator.OIDCUser{Sub: "sub-sso-1", Email: "sso@acme.dev", EmailVerified: true, Name: "SSO User"})
	idpTs := httptest.NewServer(idp.Handler())
	t.Cleanup(idpTs.Close)
	idp.SetIssuer(idpTs.URL)

	cfg := config.Load()
	cfg.SeedAcme = true
	cfg.SeedSFBaseURL = "http://sim-salesforce:9081"
	cfg.SeedHubBaseURL = "http://sim-hubspot:9082"
	cfg.SeedSFSSecret = "sfs-dev-secret"
	cfg.SeedHubSecret = "sfh-dev-secret"
	cfg.OIDCIssuer = idpTs.URL
	cfg.OIDCClientID = "syncforge-cli"
	cfg.OIDCAutoProvision = true

	c := cache.New("localhost:6379")
	metrics, err := observability.NewServiceMetrics(otel.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := api.New(cfg, database, c, metrics, logger)
	if err := srv.SeedDemoTenant(context.Background()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	ts := httptest.NewServer(srv.Router(promhttp.Handler()))
	t.Cleanup(ts.Close)
	return ts, srv, idpTs.URL
}

// mintOIDCToken asks the mock IdP for an ID token for the given username.
func mintOIDCToken(t *testing.T, idpURL, clientID, username string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"grant_type": "password", "username": username, "client_id": clientID})
	resp, err := http.Post(idpURL+"/token", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mock token endpoint: status %d", resp.StatusCode)
	}
	var out struct {
		IDToken string `json:"id_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.IDToken == "" {
		t.Fatal("mock idp returned no id_token")
	}
	return out.IDToken
}

// TestOIDCLoginAutoProvisionsAndIssuesSession proves an ID token from the
// trusted IdP logs a user in, auto-provisions a VIEWER account, and yields a
// working session token with the correct role.
func TestOIDCLoginAutoProvisionsAndIssuesSession(t *testing.T) {
	ts, _, idpURL := newOIDCAPIServer(t)

	token := mintOIDCToken(t, idpURL, "syncforge-cli", "sso@acme.dev")
	body, _ := json.Marshal(map[string]any{"tenant_slug": "acme", "id_token": token})
	resp, err := http.Post(ts.URL+"/api/v1/auth/oidc/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("oidc login: expected 200, got %d", resp.StatusCode)
	}
	var out struct {
		Token string `json:"token"`
		User  struct {
			Email string `json:"email"`
			Role  string `json:"role"`
		} `json:"user"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Token == "" {
		t.Fatal("oidc login returned no token")
	}
	if out.User.Email != "sso@acme.dev" || out.User.Role != "VIEWER" {
		t.Fatalf("unexpected provisioned user: %+v", out.User)
	}

	// The session token works against a role-gated endpoint.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/connections", nil)
	req.Header.Set("Authorization", "Bearer "+out.Token)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("session token read: expected 200, got %d", resp2.StatusCode)
	}
}

// TestOIDCLoginRejectsBadToken proves a tampered ID token is rejected.
func TestOIDCLoginRejectsBadToken(t *testing.T) {
	ts, _, _ := newOIDCAPIServer(t)

	body, _ := json.Marshal(map[string]any{"tenant_slug": "acme", "id_token": "not.a.token"})
	resp, err := http.Post(ts.URL+"/api/v1/auth/oidc/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad id token: expected 401, got %d", resp.StatusCode)
	}
}
