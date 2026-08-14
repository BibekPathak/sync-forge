//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"syncforge/internal/db"
	"syncforge/internal/store"
)

// TestRLSIsolation proves that the syncforge_app role (with Row-Level Security
// enforced) can never read another tenant's rows, even with a guessed id.
func TestRLSIsolation(t *testing.T) {
	ctx := context.Background()
	database := newDB(t)

	tenantA, err := store.CreateTenant(ctx, database.Admin, "Tenant A", "rls-a")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := store.CreateTenant(ctx, database.Admin, "Tenant B", "rls-b")
	if err != nil {
		t.Fatal(err)
	}

	// Seed a connection for each tenant via the internal (BYPASSRLS) role,
	// bypassing RLS on purpose to simulate admin provisioning.
	_, err = database.Admin.Exec(ctx,
		`INSERT INTO connections (tenant_id, name, provider, base_url, status) VALUES ($1,$2,$3,$4,$5)`,
		tenantA.ID, "SF A", "salesforce", "http://sf-a", "healthy")
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Admin.Exec(ctx,
		`INSERT INTO connections (tenant_id, name, provider, base_url, status) VALUES ($1,$2,$3,$4,$5)`,
		tenantB.ID, "SF B", "salesforce", "http://sf-b", "healthy")
	if err != nil {
		t.Fatal(err)
	}

	// Tenant A sees only its own connections.
	connsA, err := store.ListConnections(ctx, database.App, tenantA.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(connsA) != 1 {
		t.Fatalf("tenant A should see exactly 1 connection, got %d", len(connsA))
	}
	if connsA[0].TenantID != tenantA.ID {
		t.Fatalf("tenant A leaked a connection belonging to %s", connsA[0].TenantID)
	}

	// Tenant B sees only its own connections.
	connsB, err := store.ListConnections(ctx, database.App, tenantB.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(connsB) != 1 || connsB[0].TenantID != tenantB.ID {
		t.Fatalf("tenant B isolation broken: %+v", connsB)
	}

	// Even with B's connection id, tenant A cannot fetch it.
	_, err = store.GetConnection(ctx, database.App, tenantA.ID, connsB[0].ID)
	if err != store.ErrNotFound {
		t.Fatalf("tenant A must NOT read tenant B's connection (err=%v)", err)
	}

	// Direct SQL with no tenant context returns nothing (fail-closed).
	var n int
	err = database.App.QueryRow(ctx, `SELECT count(*) FROM connections`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("without a tenant context, RLS must hide all rows, got %d", n)
	}
}

// TestRLSWritesAreTenantConstrained proves an app-role session scoped to one
// tenant cannot insert rows for another tenant (WITH CHECK enforcement).
func TestRLSWritesAreTenantConstrained(t *testing.T) {
	ctx := context.Background()
	database := newDB(t)

	tenantA, err := store.CreateTenant(ctx, database.Admin, "Tenant A2", "rls-a2")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := store.CreateTenant(ctx, database.Admin, "Tenant B2", "rls-b2")
	if err != nil {
		t.Fatal(err)
	}

	err = db.WithTenantTx(ctx, database.App, tenantA.ID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO connections (tenant_id, name, provider, base_url) VALUES ($1,$2,$3,$4)`,
			tenantB.ID, "cross-tenant", "salesforce", "http://x")
		return err
	})
	if err == nil {
		t.Fatal("expected RLS to reject inserting a row for another tenant")
	}

	// And the row must not exist.
	var n int
	err = database.Admin.QueryRow(ctx,
		`SELECT count(*) FROM connections WHERE tenant_id=$1`, tenantB.ID).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("cross-tenant row leaked: %d rows", n)
	}
}
