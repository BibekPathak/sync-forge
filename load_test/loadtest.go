// Package loadtest provides a reusable load generator that drives signed
// webhooks through the SyncForge pipeline (gateway → source_events → bus →
// worker → destination sims) and reports throughput and latency. It is used by
// the integration load tests and by cmd/loadgen against a running stack.
package loadtest

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Generator fires signed webhooks at the SyncForge gateway.
type Generator struct {
	// URL is the SyncForge API base (e.g. "http://localhost:8080").
	URL string
	// WebhookSecret signs each payload (matches the source connection).
	WebhookSecret string
	// Source is the emitting provider ("salesforce" or "hubspot").
	Source string
	// TenantSlug routes the webhook ("acme").
	TenantSlug string
	// Client is the HTTP client used for the burst.
	Client *http.Client
}

// Result summarizes a load burst.
type Result struct {
	Sent        int
	Accepted    int
	Rejected    int
	Errors      int
	Elapsed     time.Duration
	Throughput  float64 // events/sec accepted
	LatenciesMS []float64
	P50         float64 // ms
	P95         float64 // ms
	P99         float64 // ms
}

// Burst sends n create webhooks concurrently. Each event gets a unique id and
// entity id derived from the prefix + index. fieldsFn builds the record body.
func (g *Generator) Burst(ctx context.Context, n, concurrency int, prefix string, fieldsFn func(i int) map[string]any) Result {
	if g.Client == nil {
		g.Client = &http.Client{Timeout: 30 * time.Second}
	}
	start := time.Now()
	latMu := sync.Mutex{}
	lat := make([]float64, 0, n)

	var (
		accepted int
		rejected int
		errors   int
		mu       sync.Mutex
	)

	limit := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		limit <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-limit }()

			entityID := fmt.Sprintf("%s-%05d", prefix, i)
			eventID := fmt.Sprintf("load-%s-%d", prefix, i)
			payload := map[string]any{
				"event_id":       eventID,
				"source":         g.Source,
				"entity_type":    "customer",
				"entity_id":      entityID,
				"event_type":     "created",
				"source_version": 1,
				"occurred_at":    start.UTC().Format(time.RFC3339Nano),
				"payload": map[string]any{
					"fields": map[string]any{
						"id": entityID, "first_name": "Load", "last_name": fmt.Sprintf("Gen%d", i),
						"email": fmt.Sprintf("%s@example.com", entityID), "phone": "+1-555-0000",
						"company": "LoadTest",
					},
				},
			}
			// Merge custom fields.
			for k, v := range fieldsFn(i) {
				payload["payload"].(map[string]any)["fields"].(map[string]any)[k] = v
			}

			body, err := json.Marshal(payload)
			if err != nil {
				mu.Lock()
				errors++
				mu.Unlock()
				return
			}

			reqStart := time.Now()
			status, err := g.send(ctx, body)
			latency := float64(time.Since(reqStart).Microseconds()) / 1000.0
			latMu.Lock()
			lat = append(lat, latency)
			latMu.Unlock()

			mu.Lock()
			switch {
			case err != nil:
				errors++
			case status == http.StatusAccepted || status == http.StatusOK:
				accepted++
			default:
				rejected++
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	sort.Float64s(lat)
	return Result{
		Sent:        n,
		Accepted:    accepted,
		Rejected:    rejected,
		Errors:      errors,
		Elapsed:     elapsed,
		Throughput:  float64(accepted) / elapsed.Seconds(),
		LatenciesMS: lat,
		P50:         percentile(lat, 0.50),
		P95:         percentile(lat, 0.95),
		P99:         percentile(lat, 0.99),
	}
}

func (g *Generator) send(ctx context.Context, body []byte) (int, error) {
	mac := hmac.New(sha256.New, []byte(g.WebhookSecret))
	mac.Write(body)
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		g.URL+"/webhooks/"+g.Source+"/"+g.TenantSlug, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-SyncForge-Signature", sig)
	resp, err := g.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	// Drain the body so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// String renders a human-readable summary of the result.
func (r Result) String() string {
	return fmt.Sprintf(
		"sent=%d accepted=%d rejected=%d errors=%d elapsed=%s throughput=%.1f ev/s p50=%.2fms p95=%.2fms p99=%.2fms",
		r.Sent, r.Accepted, r.Rejected, r.Errors, r.Elapsed.Round(time.Millisecond),
		r.Throughput, r.P50, r.P95, r.P99)
}
