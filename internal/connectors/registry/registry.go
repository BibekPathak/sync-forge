// Package registry builds provider adapters by name. It is the only place the
// core engine refers to concrete providers; adding a new provider means adding
// a case here, not touching the sync engine.
package registry

import (
	"fmt"
	"time"

	"syncforge/internal/connectors"
	"syncforge/internal/connectors/hubspot"
	"syncforge/internal/connectors/salesforce"
)

// DefaultTimeout bounds provider API calls so a hung external system cannot
// stall a sync worker indefinitely.
const DefaultTimeout = 15 * time.Second

// Provider API limits in requests/minute, mirroring the simulated providers'
// documented limits. The client token-bucket paces outbound calls so one
// tenant cannot saturate a shared connector.
var providerLimits = map[string]int{
	salesforce.Provider: 100,
	hubspot.Provider:    50,
}

// New builds the adapter for a provider, with a client-side rate limiter
// matching the provider's documented API limit.
func New(provider, baseURL, token string) (connectors.Adapter, error) {
	return NewRateLimited(provider, baseURL, token, providerLimits[provider])
}

// NewRateLimited builds the adapter for a provider with an explicit per-minute
// request limit (0 disables the client limiter).
func NewRateLimited(provider, baseURL, token string, perMinute int) (connectors.Adapter, error) {
	return NewWithTimeout(provider, baseURL, token, DefaultTimeout, perMinute)
}

// NewWithTimeout builds the adapter for a provider with an explicit per-minute
// request limit and a caller-provided API timeout. A zero perMinute disables
// the client limiter.
func NewWithTimeout(provider, baseURL, token string, timeout time.Duration, perMinute int) (connectors.Adapter, error) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	switch provider {
	case salesforce.Provider:
		return salesforce.NewRateLimited(baseURL, token, timeout, perMinute), nil
	case hubspot.Provider:
		return hubspot.NewRateLimited(baseURL, token, timeout, perMinute), nil
	default:
		return nil, fmt.Errorf("unsupported provider: %s", provider)
	}
}
