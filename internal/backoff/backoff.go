// Package backoff provides exponential-with-jitter retry delays used by the
// worker's durable handoff and the retry engine. It is a leaf package (no
// SyncForge dependencies) so both can import it without cycles.
package backoff

import (
	"math/rand/v2"
	"time"
)

// ComputeDelay returns the exponential backoff delay for a failure count
// (1-based): base, 2*base, 4*base, ... capped at max with up to 30% uniform
// jitter so concurrent retries do not synchronize into waves.
func ComputeDelay(failures int, base, max time.Duration) time.Duration {
	if failures < 1 {
		failures = 1
	}
	if base <= 0 {
		base = 1 * time.Second
	}
	if max <= 0 {
		max = 60 * time.Second
	}
	exp := base * time.Duration(1<<(failures-1))
	if exp > max || exp <= 0 {
		exp = max
	}
	// Multiply by a jitter factor in [0.7, 1.3], then clamp to the cap so the
	// final scheduled delay never exceeds max.
	factor := 0.7 + rand.Float64()*0.6
	delay := time.Duration(float64(exp) * factor)
	if delay > max {
		delay = max
	}
	return delay
}
