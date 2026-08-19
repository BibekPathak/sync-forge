package oidc

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"syncforge/internal/simulator"
)

// TestVerifyIDTokenRoundTrip proves the client can discover the mock IdP, fetch
// its JWKS, and verify a signed ID token (issuer/audience/expiry/signature).
func TestVerifyIDTokenRoundTrip(t *testing.T) {
	const clientID = "syncforge-cli"
	idp, err := simulator.NewOIDCProvider("http://idp.test", clientID)
	if err != nil {
		t.Fatal(err)
	}
	idp.AddUser(simulator.OIDCUser{Sub: "user-1", Email: "sso@acme.dev", EmailVerified: true, Name: "SSO User"})
	idp.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	ts := httptest.NewServer(idp.Handler())
	defer ts.Close()
	idp.SetIssuer(ts.URL)

	// Point discovery at the httptest URL.
	issuer := ts.URL
	prov, err := NewProvider(context.Background(), Config{Issuer: issuer, ClientID: clientID})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}

	// Mint a token via the mock token endpoint.
	tok := mintIDToken(t, ts.URL, clientID, "user-1")
	claims, err := prov.VerifyIDToken(context.Background(), tok)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.Sub != "user-1" || claims.Email != "sso@acme.dev" || !claims.EmailVerified {
		t.Fatalf("unexpected claims: %+v", claims)
	}
	if claims.Issuer != issuer {
		t.Fatalf("issuer mismatch: %q", claims.Issuer)
	}
}

// TestVerifyIDTokenRejectsTampering proves a modified token fails signature
// verification.
func TestVerifyIDTokenRejectsTampering(t *testing.T) {
	const clientID = "syncforge-cli"
	idp, err := simulator.NewOIDCProvider("http://idp.test", clientID)
	if err != nil {
		t.Fatal(err)
	}
	idp.AddUser(simulator.OIDCUser{Sub: "user-1", Email: "sso@acme.dev"})
	ts := httptest.NewServer(idp.Handler())
	defer ts.Close()
	idp.SetIssuer(ts.URL)

	prov, err := NewProvider(context.Background(), Config{Issuer: ts.URL, ClientID: clientID})
	if err != nil {
		t.Fatal(err)
	}
	tok := mintIDToken(t, ts.URL, clientID, "user-1")
	tampered := tok[:len(tok)-2] + "XY"
	if claims, err := prov.VerifyIDToken(context.Background(), tampered); err == nil {
		t.Fatalf("expected tampered token to fail, got claims %+v", claims)
	}
}

// TestVerifyIDTokenRejectsWrongAudience proves audience is enforced.
func TestVerifyIDTokenRejectsWrongAudience(t *testing.T) {
	const clientID = "syncforge-cli"
	idp, err := simulator.NewOIDCProvider("http://idp.test", clientID)
	if err != nil {
		t.Fatal(err)
	}
	idp.AddUser(simulator.OIDCUser{Sub: "user-1", Email: "sso@acme.dev"})
	ts := httptest.NewServer(idp.Handler())
	defer ts.Close()
	idp.SetIssuer(ts.URL)

	prov, err := NewProvider(context.Background(), Config{Issuer: ts.URL, ClientID: clientID})
	if err != nil {
		t.Fatal(err)
	}
	// Mint for a different client id -> audience mismatch.
	tok := mintIDTokenForClient(t, ts.URL, "some-other-app", "user-1")
	if claims, err := prov.VerifyIDToken(context.Background(), tok); err == nil {
		t.Fatalf("expected wrong-audience token to fail, got claims %+v", claims)
	}
}

func mintIDToken(t *testing.T, issuer, clientID, username string) string {
	return mintIDTokenForClient(t, issuer, clientID, username)
}

func mintIDTokenForClient(t *testing.T, issuer, clientID, username string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"grant_type": "password", "username": username, "client_id": clientID})
	req, err := http.NewRequest(http.MethodPost, issuer+"/token", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("token endpoint: status %d", resp.StatusCode)
	}
	var out struct {
		IDToken string `json:"id_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.IDToken == "" {
		t.Fatal("no id_token returned")
	}
	return out.IDToken
}
