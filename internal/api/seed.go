package api

import (
	"context"
	"fmt"

	"golang.org/x/crypto/bcrypt"

	"syncforge/internal/store"
)

// Known demo API key. In production these are generated and returned once.
const demoAPIKey = "sfk_acme_dev"

// Known demo user credentials (login via POST /api/v1/auth/login).
const (
	demoUserEmail    = "admin@acme.dev"
	demoUserPassword = "syncforge-demo"
)

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

	// Seed a demo ADMIN user so the login flow is usable out of the box.
	if _, err := store.GetUserByEmail(ctx, s.db.Admin, tenant.ID, demoUserEmail); err == store.ErrNotFound {
		hash, err := bcrypt.GenerateFromPassword([]byte(demoUserPassword), bcrypt.DefaultCost)
		if err != nil {
			return struct{}{}, fmt.Errorf("seed user password: %w", err)
		}
		if _, err := store.CreateUser(ctx, s.db.Admin, tenant.ID, demoUserEmail, string(hash), "ADMIN"); err != nil {
			return struct{}{}, fmt.Errorf("seed user: %w", err)
		}
		s.log.Info("seeded demo user", "tenant", tenant.Slug, "email", demoUserEmail)
	}

	// Bidirectional policies: customers flow both ways.
	_, err = store.UpsertSyncPolicy(ctx, s.db.App, store.SyncPolicy{
		TenantID:         tenant.ID,
		Entity:           "customer",
		Source:           "salesforce",
		Destination:      "hubspot",
		Mode:             "bidirectional",
		ConflictStrategy: "field_merge",
		DeletePolicy:     "propagate",
		RetryPolicy:      "exponential_backoff",
		SourcePriority:   100,
		Enabled:          true,
	})
	if err != nil {
		return struct{}{}, fmt.Errorf("seed sync policy: %w", err)
	}
	_, err = store.UpsertSyncPolicy(ctx, s.db.App, store.SyncPolicy{
		TenantID:         tenant.ID,
		Entity:           "customer",
		Source:           "hubspot",
		Destination:      "salesforce",
		Mode:             "bidirectional",
		ConflictStrategy: "field_merge",
		DeletePolicy:     "propagate",
		RetryPolicy:      "exponential_backoff",
		SourcePriority:   200,
		Enabled:          true,
	})
	if err != nil {
		return struct{}{}, fmt.Errorf("seed sync policy: %w", err)
	}
	s.log.Info("seeded sync policies", "tenant", tenant.Slug, "entity", "customer", "mode", "bidirectional")

	return struct{}{}, nil
}
