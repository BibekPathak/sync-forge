package simulator

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// OIDCProvider is a mock OpenID Connect identity provider used for local
// development and integration tests. It serves discovery, JWKS, and a token
// endpoint that issues RS256-signed ID tokens for known test users, mirroring
// how the provider simulators mock external systems.
type OIDCProvider struct {
	Issuer   string
	ClientID string
	key      *rsa.PrivateKey
	kid      string
	users    map[string]OIDCUser // sub -> user
	Log      *slog.Logger
}

// OIDCUser is a known identity the mock IdP will issue tokens for.
type OIDCUser struct {
	Sub           string
	Email         string
	EmailVerified bool
	Name          string
}

// NewOIDCProvider builds a mock IdP with a fresh RSA key.
func NewOIDCProvider(issuer, clientID string) (*OIDCProvider, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	return &OIDCProvider{
		Issuer:   issuer,
		ClientID: clientID,
		key:      key,
		kid:      "mock-idp-1",
		users:    map[string]OIDCUser{},
	}, nil
}

// AddUser registers a known identity.
func (p *OIDCProvider) AddUser(u OIDCUser) { p.users[u.Sub] = u }

// SetIssuer updates the advertised issuer (used by tests that serve the mock
// at an httptest URL and want the client to discover it there).
func (p *OIDCProvider) SetIssuer(issuer string) { p.Issuer = issuer }

// Handler returns the mock IdP's HTTP surface.
func (p *OIDCProvider) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", p.handleDiscovery)
	mux.HandleFunc("/jwks", p.handleJWKS)
	mux.HandleFunc("/token", p.handleToken)
	return mux
}

func (p *OIDCProvider) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"issuer":                                p.Issuer,
		"jwks_uri":                              p.Issuer + "/jwks",
		"token_endpoint":                        p.Issuer + "/token",
		"authorization_endpoint":                p.Issuer + "/authorize",
		"id_token_signing_alg_values_supported": []string{"RS256"},
	})
}

func (p *OIDCProvider) handleJWKS(w http.ResponseWriter, r *http.Request) {
	pub := &p.key.PublicKey
	n := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString([]byte{0x01, 0x00, 0x01})
	writeJSON(w, http.StatusOK, map[string]any{
		"keys": []map[string]any{{
			"kty": "RSA",
			"kid": p.kid,
			"use": "sig",
			"alg": "RS256",
			"n":   n,
			"e":   e,
		}},
	})
}

type tokenRequest struct {
	GrantType string `json:"grant_type"`
	Username  string `json:"username"`  // sub or email for the mock
	ClientID  string `json:"client_id"` // optional audience override
}

// handleToken mints an ID token for a known user (resource-owner style, for
// tests). It returns an id_token that the SyncForge oidc client can verify.
func (p *OIDCProvider) handleToken(w http.ResponseWriter, r *http.Request) {
	var req tokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid_request"}`, http.StatusBadRequest)
		return
	}
	aud := p.ClientID
	if req.ClientID != "" {
		aud = req.ClientID
	}
	var user OIDCUser
	found := false
	for _, u := range p.users {
		if u.Sub == req.Username || u.Email == req.Username {
			user, found = u, true
			break
		}
	}
	if !found {
		http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
		return
	}

	now := time.Now().Unix()
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "kid": p.kid, "typ": "JWT"})
	payload, _ := json.Marshal(map[string]any{
		"iss":            p.Issuer,
		"sub":            user.Sub,
		"aud":            aud,
		"exp":            now + 3600,
		"iat":            now,
		"email":          user.Email,
		"email_verified": user.EmailVerified,
		"name":           user.Name,
	})
	signingInput := b64url(header) + "." + b64url(payload)
	digest := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, p.key, crypto.SHA256, digest[:])
	if err != nil {
		http.Error(w, `{"error":"server_error"}`, http.StatusInternalServerError)
		return
	}
	idToken := signingInput + "." + b64url(sig)
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token": "mock-access",
		"token_type":   "Bearer",
		"expires_in":   3600,
		"id_token":     idToken,
	})
}

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
