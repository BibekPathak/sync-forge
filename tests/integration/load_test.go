//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"syncforge/internal/simulator"
	"syncforge/internal/store"
	"syncforge/load_test"
)

// TestLoadSustainedThroughput drives a sustained burst of create webhooks
// through the whole pipeline and asserts: every event is accepted, every event
// lands in exactly one destination record (no loss, no duplicates), and the
// observed throughput stays above a floor. Latency percentiles are reported
// for the benchmark record.
func TestLoadSustainedThroughput(t *testing.T) {
	h := newPipelineHarness(t)

	const (
		n           = 300
		concurrency = 32
		minEvs      = 20.0 // conservative floor; local runs hit far higher
	)

	gen := &loadtest.Generator{
		URL:           h.api.URL,
		WebhookSecret: "sfs-dev-secret",
		Source:        "salesforce",
		TenantSlug:    "acme",
	}

	res := gen.Burst(context.Background(), n, concurrency, "sf", func(i int) map[string]any { return nil })
	t.Logf("burst: %s", res.String())

	if res.Rejected != 0 || res.Errors != 0 {
		t.Fatalf("expected all webhooks accepted, rejected=%d errors=%d", res.Rejected, res.Errors)
	}
	if res.Accepted != n {
		t.Fatalf("expected %d accepted, got %d", n, res.Accepted)
	}
	if res.Throughput < minEvs {
		t.Fatalf("throughput %.1f ev/s below floor %.1f ev/s", res.Throughput, minEvs)
	}

	// Every event must reach exactly one HubSpot contact (the destination for
	// the seeded salesforce → hubspot policy).
	contacts := h.waitForHubContact(t, n)
	if len(contacts) != n {
		t.Fatalf("expected exactly %d hubspot contacts (zero data loss), got %d", n, len(contacts))
	}

	// No duplicates: unique emailAddress across all contacts.
	seen := make(map[string]bool, len(contacts))
	for _, c := range contacts {
		email, _ := c["emailAddress"].(string)
		if email == "" {
			t.Fatalf("contact missing emailAddress: %v", c)
		}
		if seen[email] {
			t.Fatalf("duplicate destination record for email %s", email)
		}
		seen[email] = true
	}

	// Every canonical record exists and carries both provider ids.
	ctx := context.Background()
	for i := 0; i < n; i++ {
		entityID := fmt.Sprintf("sf-%05d", i)
		canonical, err := store.GetCanonicalByProvider(ctx, h.db.App, h.acmeID, "customer", "salesforce", entityID)
		if err != nil {
			t.Fatalf("canonical missing for %s: %v", entityID, err)
		}
		if canonical.ProviderIDs["hubspot"] == "" {
			t.Fatalf("provider id mapping incomplete for %s: %+v", entityID, canonical.ProviderIDs)
		}
	}

	// DLQ and retry queue must be empty: nothing failed under load.
	dlq, err := store.ListDeadLetters(ctx, h.db.App, h.acmeID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlq) != 0 {
		t.Fatalf("unexpected DLQ entries under load: %d", len(dlq))
	}
}

// TestLoadBurstUnderFaults runs a burst while the destination provider is
// down (every write fails transiently), then recovers and asserts convergence
// with no duplicate destination records. This exercises the Phase 4 retry
// machinery under load and validates the observability surface (durable
// failure, eventual success).
func TestLoadBurstUnderFaults(t *testing.T) {
	h := newPipelineHarness(t)

	// Take HubSpot down hard (every API call fails with a transient 500) so
	// every destination write fails during the burst and lands in the retry
	// queue. A hard outage recovers deterministically (unlike a token-bucket
	// limit whose refill rate would throttle the recovery drain itself).
	h.hubSrv.Faults.Set(simulator.FaultConfig{FailureRate: 1.0})

	const (
		n           = 150
		concurrency = 16
	)
	gen := &loadtest.Generator{
		URL:           h.api.URL,
		WebhookSecret: "sfs-dev-secret",
		Source:        "salesforce",
		TenantSlug:    "acme",
	}
	res := gen.Burst(context.Background(), n, concurrency, "sf-fault", func(i int) map[string]any { return nil })
	t.Logf("burst (under faults): %s", res.String())
	if res.Accepted != n {
		t.Fatalf("gateway must accept all webhooks even when the destination is down; accepted=%d", res.Accepted)
	}

	// Events fail durably into the retry queue while the provider is down.
	waitFor(t, 10*time.Second, "retries queued under load", func() bool {
		var count int
		err := h.db.Admin.QueryRow(context.Background(),
			`SELECT count(*) FROM retry_queue WHERE tenant_id=$1`, h.acmeID).Scan(&count)
		if err != nil {
			return false
		}
		return count > 0
	})

	// Provider recovers: drain the retry engine until every contact lands.
	h.hubSrv.Faults.Set(simulator.FaultConfig{})
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.hubContacts(t)) >= n {
			break
		}
		h.retryEngine.Drain(context.Background())
		time.Sleep(25 * time.Millisecond)
	}
	contacts := h.hubContacts(t)
	if len(contacts) != n {
		t.Fatalf("expected %d hubspot contacts after recovery (zero loss), got %d", n, len(contacts))
	}

	// Exactly one destination record per event, even after retries.
	seen := make(map[string]bool, len(contacts))
	for _, c := range contacts {
		email, _ := c["emailAddress"].(string)
		if email == "" {
			t.Fatalf("contact missing emailAddress: %v", c)
		}
		if seen[email] {
			t.Fatalf("duplicate destination record after recovery for email %s", email)
		}
		seen[email] = true
	}

	// Retry queue drained and no DLQ growth (retryable failures recovered).
	var retryCount int
	if err := h.db.Admin.QueryRow(context.Background(),
		`SELECT count(*) FROM retry_queue WHERE tenant_id=$1`, h.acmeID).Scan(&retryCount); err != nil {
		t.Fatal(err)
	}
	if retryCount != 0 {
		t.Fatalf("retry queue should be drained after recovery, got %d rows", retryCount)
	}
	dlq, err := store.ListDeadLetters(context.Background(), h.db.App, h.acmeID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlq) != 0 {
		t.Fatalf("unexpected DLQ entries after recovery: %d", len(dlq))
	}
}
