// Command loadgen fires a burst of signed webhooks at a running SyncForge
// gateway and reports throughput/latency. It exercises the whole ingestion →
// worker → destination path end-to-end. Example:
//
//	SYNCFORGE_API_URL=http://localhost:8080 go run ./cmd/loadgen -n 500 -c 32 -source salesforce
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"syncforge/load_test"
)

func main() {
	var (
		url     = flag.String("url", envOr("SYNCFORGE_API_URL", "http://localhost:8080"), "SyncForge API base URL")
		secret  = flag.String("secret", envOr("SYNCFORGE_WEBHOOK_SECRET", "sfs-dev-secret"), "webhook signing secret")
		source  = flag.String("source", envOr("SYNCFORGE_SOURCE", "salesforce"), "source provider slug")
		slug    = flag.String("tenant", envOr("SYNCFORGE_TENANT_SLUG", "acme"), "tenant slug")
		n       = flag.Int("n", 500, "number of webhooks to fire")
		conc    = flag.Int("c", 32, "concurrency")
		timeout = flag.Duration("timeout", 5*time.Minute, "burst deadline")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	gen := &loadtest.Generator{
		URL:           *url,
		WebhookSecret: *secret,
		Source:        *source,
		TenantSlug:    *slug,
	}
	res := gen.Burst(ctx, *n, *conc, *source, func(i int) map[string]any { return nil })
	fmt.Println(res.String())

	if res.Accepted == 0 && res.Rejected == 0 && res.Errors == 0 {
		fmt.Fprintln(os.Stderr, "error: no requests completed; is the gateway reachable?")
		os.Exit(1)
	}
	if res.Rejected > 0 || res.Errors > 0 {
		fmt.Printf("rejected=%d errors=%d — events will land in source_events with non-202 status\n", res.Rejected, res.Errors)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
