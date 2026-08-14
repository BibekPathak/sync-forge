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

// New builds the adapter for a provider.
func New(provider, baseURL, token string) (connectors.Adapter, error) {
	switch provider {
	case salesforce.Provider:
		return salesforce.New(baseURL, token, DefaultTimeout), nil
	case hubspot.Provider:
		return hubspot.New(baseURL, token, DefaultTimeout), nil
	default:
		return nil, fmt.Errorf("unsupported provider: %s", provider)
	}
}
