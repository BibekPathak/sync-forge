package api

import (
	"context"
	"fmt"

	"syncforge/internal/store"
)

// Known demo API key. In production these are generated and returned once.
const demoAPIKey = "sfk_acme_dev"

// seed idempotently provisions the demo tenant and its fixtures.
func (s *Server) seed(ctx context.Context) (struct{}, error) {
	tenant, err := store.GetTenantBySlug(ctx, s.db.Admin, "acme")
	if err == store.ErrNotFound {
		tenant, err = store.CreateTenant(ctx, s.db.Admin, "Acme Corp", "acme")
		if err != nil && err != store.ErrExists {
			return struct{}{}, err
		}
		s.log.Info("seeded tenant", "tenant_id", tenant.ID, "slug", tenant.Slug)
	} else if err != nil {
		return struct{}{}, err
	}

	if _, err := store.GetConnectionByProvider(ctx, s.db.App, tenant.ID, "salesforce"); err == store.ErrNotFound {
		_, err = store.CreateConnection(ctx, s.db.App, store.Connection{
			TenantID:      tenant.ID,
			Name:          "Salesforce (simulated)",
			Provider:      "salesforce",
			BaseURL:       s.cfg.SeedSFBaseURL,
			Status:        "disconnected",
			WebhookSecret: s.cfg.SeedSFSSecret,
		})
		if err != nil {
			return struct{}{}, fmt.Errorf("seed salesforce connection: %w", err)
		}
		s.log.Info("seeded salesforce connection")
	}

	if _, err := store.GetConnectionByProvider(ctx, s.db.App, tenant.ID, "hubspot"); err == store.ErrNotFound {
		_, err = store.CreateConnection(ctx, s.db.App, store.Connection{
			TenantID:      tenant.ID,
			Name:          "HubSpot (simulated)",
			Provider:      "hubspot",
			BaseURL:       s.cfg.SeedHubBaseURL,
			Status:        "disconnected",
			WebhookSecret: s.cfg.SeedHubSecret,
		})
		if err != nil {
			return struct{}{}, fmt.Errorf("seed hubspot connection: %w", err)
		}
		s.log.Info("seeded hubspot connection")
	}

	if _, err := store.VerifyAPIKey(ctx, s.db.Admin, hashAPIKey(demoAPIKey)); err == store.ErrNotFound {
		_, err = store.CreateAPIKey(ctx, s.db.Admin, tenant.ID, "demo-key", "ADMIN", hashAPIKey(demoAPIKey))
		if err != nil {
			return struct{}{}, fmt.Errorf("seed api key: %w", err)
		}
		s.log.Info("seeded api key", "tenant", tenant.Slug)
	}

	// Default policy: customers flow from Salesforce to HubSpot.
	_, err = store.UpsertSyncPolicy(ctx, s.db.App, store.SyncPolicy{
		TenantID:         tenant.ID,
		Entity:           "customer",
		Source:           "salesforce",
		Destination:      "hubspot",
		Mode:             "one_way",
		ConflictStrategy: "field_merge",
		DeletePolicy:     "propagate",
		RetryPolicy:      "exponential_backoff",
		SourcePriority:   100,
		Enabled:          true,
	})
	if err != nil {
		return struct{}{}, fmt.Errorf("seed sync policy: %w", err)
	}
	s.log.Info("seeded sync policy", "tenant", tenant.Slug, "entity", "customer", "flow", "salesforce->hubspot")

	return struct{}{}, nil
}
