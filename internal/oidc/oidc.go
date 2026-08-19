// Package oidc implements a minimal OIDC client: discovery, JWKS-based RS256
// ID-token verification, and claim extraction. It is deliberately
// dependency-free (stdlib crypto only) and mirrors the project's approach to
// TOTP. It is enough for verifying ID tokens issued by standard IdPs (and the
// bundled mock provider).
package oidc

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"
)

// Config configures an OIDC client.
type Config struct {
	Issuer   string
	ClientID string
}

// Provider is a discovered OIDC provider (issuer + endpoints + keys).
type Provider struct {
	cfg       Config
	issuer    string
	jwksURI   string
	client    *http.Client
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
}

// Claims is the decoded ID-token payload relevant to SyncForge.
type Claims struct {
	Sub           string `json:"sub"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	Name          string `json:"name"`
	Issuer        string `json:"iss"`
	Audience      any    `json:"aud"`
	ExpiresAt     int64  `json:"exp"`
	IssuedAt      int64  `json:"iat"`
}

// NewProvider performs OIDC discovery against the issuer and fetches its JWKS.
func NewProvider(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("oidc: issuer is required")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	issuer := strings.TrimSuffix(cfg.Issuer, "/")

	var disc struct {
		Issuer        string `json:"issuer"`
		JWKSURI       string `json:"jwks_uri"`
		TokenEndpoint string `json:"token_endpoint"`
	}
	u := issuer + "/.well-known/openid-configuration"
	if err := getJSON(ctx, client, u, &disc); err != nil {
		return nil, fmt.Errorf("oidc discovery %s: %w", u, err)
	}
	if disc.Issuer != issuer {
		return nil, fmt.Errorf("oidc: issuer mismatch (discovered %q want %q)", disc.Issuer, issuer)
	}
	p := &Provider{cfg: cfg, issuer: issuer, jwksURI: disc.JWKSURI, client: client, keys: map[string]*rsa.PublicKey{}}
	if err := p.refreshKeys(ctx); err != nil {
		return nil, fmt.Errorf("oidc fetch jwks: %w", err)
	}
	return p, nil
}

func getJSON(ctx context.Context, client *http.Client, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// refreshKeys fetches and caches the provider's RSA public keys (JWKS).
func (p *Provider) refreshKeys(ctx context.Context) error {
	if p.jwksURI == "" {
		return errors.New("oidc: provider has no jwks_uri")
	}
	var jwks struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := getJSON(ctx, p.client, p.jwksURI, &jwks); err != nil {
		return err
	}
	keys := map[string]*rsa.PublicKey{}
	for _, k := range jwks.Keys {
		if k.Kty != "RSA" || k.Kid == "" {
			continue
		}
		pub, err := jwkRSAPublicKey(k.N, k.E)
		if err != nil {
			return err
		}
		keys[k.Kid] = pub
	}
	if len(keys) == 0 {
		return errors.New("oidc: no RSA keys in JWKS")
	}
	p.keys = keys
	p.fetchedAt = time.Now()
	return nil
}

func jwkRSAPublicKey(nB64, eB64 string) (*rsa.PublicKey, error) {
	nRaw, err := base64.RawURLEncoding.DecodeString(nB64)
	if err != nil {
		return nil, err
	}
	eRaw, err := base64.RawURLEncoding.DecodeString(eB64)
	if err != nil {
		return nil, err
	}
	e := big.NewInt(0).SetBytes(eRaw)
	n := big.NewInt(0).SetBytes(nRaw)
	if !e.IsInt64() {
		return nil, errors.New("oidc: invalid RSA exponent")
	}
	return &rsa.PublicKey{N: n, E: int(e.Int64())}, nil
}

// VerifyIDToken validates an ID token's signature (RS256, against the cached
// JWKS), issuer, audience, and expiry, and returns its claims.
func (p *Provider) VerifyIDToken(ctx context.Context, idToken string) (Claims, error) {
	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		return Claims{}, errors.New("oidc: malformed id_token")
	}
	headerRaw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Claims{}, errors.New("oidc: malformed header")
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(headerRaw, &header); err != nil {
		return Claims{}, err
	}
	if header.Alg != "RS256" {
		return Claims{}, fmt.Errorf("oidc: unsupported alg %q", header.Alg)
	}
	pub, ok := p.keys[header.Kid]
	if !ok {
		// Key rotation: refresh once and retry.
		if err := p.refreshKeys(ctx); err != nil {
			return Claims{}, err
		}
		pub, ok = p.keys[header.Kid]
		if !ok {
			return Claims{}, fmt.Errorf("oidc: unknown key id %q", header.Kid)
		}
	}
	signingInput := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Claims{}, errors.New("oidc: malformed signature")
	}
	digest := sha256.Sum256([]byte(signingInput))
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig); err != nil {
		return Claims{}, errors.New("oidc: invalid id_token signature")
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, errors.New("oidc: malformed payload")
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, err
	}
	if c.Issuer != p.issuer {
		return Claims{}, fmt.Errorf("oidc: issuer mismatch (token %q want %q)", c.Issuer, p.issuer)
	}
	if !audienceMatches(c.Audience, p.cfg.ClientID) {
		return Claims{}, fmt.Errorf("oidc: audience mismatch %v", c.Audience)
	}
	if c.ExpiresAt > 0 && c.ExpiresAt < time.Now().Unix() {
		return Claims{}, errors.New("oidc: id_token expired")
	}
	if c.Sub == "" || c.Email == "" {
		return Claims{}, errors.New("oidc: id_token missing sub/email")
	}
	return c, nil
}

func audienceMatches(aud any, clientID string) bool {
	switch v := aud.(type) {
	case string:
		return v == clientID
	case []any:
		for _, a := range v {
			if s, ok := a.(string); ok && s == clientID {
				return true
			}
		}
	case []string:
		for _, a := range v {
			if a == clientID {
				return true
			}
		}
	}
	return false
}
