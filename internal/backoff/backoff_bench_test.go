package backoff

import (
	"testing"
	"time"
)

// BenchmarkComputeDelay measures the common retry-backoff path.
func BenchmarkComputeDelay(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d := ComputeDelay((i%8)+1, 1*time.Second, 60*time.Second)
		if d <= 0 {
			b.Fatalf("expected positive delay, got %v", d)
		}
	}
}

// BenchmarkComputeDelayCapped measures the ceiling-hit path at high attempt
// counts.
func BenchmarkComputeDelayCapped(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d := ComputeDelay(12, 1*time.Second, 5*time.Second)
		if d <= 0 || d > 5*time.Second {
			b.Fatalf("delay out of range: %v", d)
		}
	}
}
