package hubspot

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"syncforge/internal/connectors"
	"syncforge/internal/model"
	"syncforge/internal/simulator"
)

func newTestSim(t *testing.T) *httptest.Server {
	t.Helper()
	spec := &simulator.Spec{
		Name:       "hubspot",
		EntityType: "contact",
		IDKey:      "contact_id",
		TimeKey:    "modifiedAt",
		IDPrefix:   "hub-",
		Path:       "/contacts",
	}
	s := simulator.NewServer(spec, simulator.Options{
		RateLimitPerMin: 50,
		SeedCount:       10,
		SeedRec: func(id string, n int) map[string]any {
			return map[string]any{
				"firstName":    "Ada",
				"lastName":     "Lovelace",
				"emailAddress": "ada@acme.io",
				"phoneNumber":  "+44-20-0000",
				"organization": "Analytical",
			}
		},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func TestHubSpotListAndSchema(t *testing.T) {
	ts := newTestSim(t)
	c := New(ts.URL, "", 5*time.Second)
	ctx := context.Background()

	page, err := c.List(ctx, connectors.ListOptions{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 10 {
		t.Fatalf("expected 10 records, got %d", len(page.Records))
	}

	rec := page.Records[0]
	if rec.ID == "" {
		t.Fatal("hubspot record missing contact_id mapping")
	}

	// normalize the hubspot native schema
	cust, err := c.Normalize(rec)
	if err != nil {
		t.Fatal(err)
	}
	if cust.FirstName != "Ada" || cust.Email != "ada@acme.io" || cust.Company != "Analytical" {
		t.Fatalf("hubspot normalize mismatch: %+v", cust)
	}
	if cust.SourceVersions["hubspot"] != rec.SourceVersion {
		t.Fatal("hubspot source version not preserved")
	}
}

func TestHubSpotDenormalize(t *testing.T) {
	c := New("http://unused", "", time.Second)
	cust := &model.Customer{
		EntityID:  "hub-1",
		FirstName: "Grace",
		LastName:  "Hopper",
		Email:     "grace@acme.io",
		Phone:     "+44-20-1111",
		Company:   "Navy",
	}
	rec, err := c.Denormalize(cust)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Data["firstName"] != "Grace" {
		t.Fatalf("expected camelCase firstName, got %v", rec.Data["firstName"])
	}
	if rec.Data["emailAddress"] != "grace@acme.io" {
		t.Fatalf("expected emailAddress, got %v", rec.Data["emailAddress"])
	}
	if rec.Data["organization"] != "Navy" {
		t.Fatalf("expected organization, got %v", rec.Data["organization"])
	}
}

func TestHubSpotHealth(t *testing.T) {
	ts := newTestSim(t)
	c := New(ts.URL, "", 5*time.Second)
	h, err := c.HealthCheck(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if h.Status != "healthy" {
		t.Fatalf("unexpected health: %v", h.Status)
	}
}
