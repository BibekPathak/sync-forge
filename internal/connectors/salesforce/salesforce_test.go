package salesforce

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
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
		Name:       "salesforce",
		EntityType: "customer",
		IDKey:      "id",
		TimeKey:    "updated_at",
		IDPrefix:   "sf-",
		Path:       "/customers",
	}
	s := simulator.NewServer(spec, simulator.Options{
		RateLimitPerMin: 100,
		SeedCount:       25,
		SeedRec: func(id string, n int) map[string]any {
			return map[string]any{
				"first_name": "Ada",
				"last_name":  "Lovelace",
				"email":      "ada@example.com",
				"phone":      "+1-555-0000",
				"company":    "Analytical",
			}
		},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func TestHealthCheck(t *testing.T) {
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

func TestListPagination(t *testing.T) {
	ts := newTestSim(t)
	c := New(ts.URL, "", 5*time.Second)
	ctx := context.Background()

	var all []connectors.ProviderRecord
	cursor := ""
	for {
		page, err := c.List(ctx, connectors.ListOptions{Cursor: cursor, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Records...)
		if !page.HasMore {
			break
		}
		cursor = page.NextCursor
	}
	if len(all) != 25 {
		t.Fatalf("expected 25 records, got %d", len(all))
	}
	for _, rec := range all {
		if rec.ID == "" {
			t.Fatal("record missing id")
		}
		if rec.SourceVersion != 1 {
			t.Fatalf("expected version 1, got %d", rec.SourceVersion)
		}
	}
}

func TestCRUD(t *testing.T) {
	ts := newTestSim(t)
	c := New(ts.URL, "", 5*time.Second)
	ctx := context.Background()

	created, err := c.Create(ctx, connectors.ProviderRecord{Data: map[string]any{
		"first_name": "Grace",
		"last_name":  "Hopper",
		"email":      "grace@example.com",
		"phone":      "+1-555-2222",
		"company":    "Navy",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.SourceVersion != 1 {
		t.Fatalf("unexpected created record: %+v", created)
	}

	updated, err := c.Update(ctx, created.ID, connectors.ProviderRecord{Data: map[string]any{
		"email": "grace.hopper@example.com",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if updated.SourceVersion != 2 {
		t.Fatalf("expected version 2 after update, got %d", updated.SourceVersion)
	}

	fetched, err := c.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.Data["email"] != "grace.hopper@example.com" {
		t.Fatalf("email not updated: %v", fetched.Data["email"])
	}

	if err := c.Delete(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	gone, err := c.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !gone.Deleted {
		t.Fatal("expected tombstone after delete")
	}
}

func TestNormalizeDenormalizeRoundtrip(t *testing.T) {
	c := New("http://unused", "", time.Second)
	cust := &model.Customer{
		EntityID:  "sf-1",
		FirstName: "Margaret",
		LastName:  "Hamilton",
		Email:     "margaret@example.com",
		Phone:     "+1-555-3333",
		Company:   "NASA",
	}
	rec, err := c.Denormalize(cust)
	if err != nil {
		t.Fatal(err)
	}
	rec.ID = "sf-1"
	rec.SourceVersion = 7
	rec.Data["version"] = 7
	rec.Data["updated_at"] = "2024-01-01T00:00:00Z"

	back, err := c.Normalize(rec)
	if err != nil {
		t.Fatal(err)
	}
	if back.FirstName != "Margaret" || back.LastName != "Hamilton" || back.Email != "margaret@example.com" {
		t.Fatalf("normalize mismatch: %+v", back)
	}
	if back.Phone != "+1-555-3333" || back.Company != "NASA" {
		t.Fatalf("normalize mismatch: %+v", back)
	}
	if back.SourceVersions["salesforce"] != 7 {
		t.Fatalf("source version not preserved: %+v", back.SourceVersions)
	}
}

func TestValidateMissingField(t *testing.T) {
	c := New("http://unused", "", time.Second)
	err := c.Validate(connectors.ProviderRecord{Data: map[string]any{"first_name": "A"}})
	if !connectors.IsKind(err, connectors.ErrSchema) {
		t.Fatalf("expected schema error, got %v", err)
	}
}

func TestRateLimitErrorClassification(t *testing.T) {
	ts := newTestSim(t)
	c := New(ts.URL, "", 5*time.Second)
	ctx := context.Background()

	// Lower the provider's per-minute capacity via fault injection.
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/faults",
		bytes.NewBufferString(`{"rate_limit_per_min": 2}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	for i := 0; i < 2; i++ {
		if _, err := c.List(ctx, connectors.ListOptions{Limit: 10}); err != nil {
			t.Fatalf("request %d should succeed: %v", i, err)
		}
	}
	_, err = c.List(ctx, connectors.ListOptions{Limit: 10})
	if !connectors.IsKind(err, connectors.ErrRateLimited) {
		t.Fatalf("expected rate limited error, got %v", err)
	}
}
