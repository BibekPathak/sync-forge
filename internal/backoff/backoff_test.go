package backoff

import (
	"testing"
	"time"
)

func TestComputeDelayBaseAndDoubling(t *testing.T) {
	base := time.Second
	max := 60 * time.Second

	// failures=1 -> base within jitter range [0.7s, 1.3s].
	d1 := ComputeDelay(1, base, max)
	if d1 < 700*time.Millisecond || d1 > 1300*time.Millisecond {
		t.Fatalf("delay(1)=%v outside expected jitter band", d1)
	}

	// failures=2 -> ~2*base within [1.4s, 2.6s].
	d2 := ComputeDelay(2, base, max)
	if d2 < 1400*time.Millisecond || d2 > 2600*time.Millisecond {
		t.Fatalf("delay(2)=%v outside expected jitter band", d2)
	}

	// failures=3 -> ~4*base.
	d3 := ComputeDelay(3, base, max)
	if d3 < 2800*time.Millisecond || d3 > 5200*time.Millisecond {
		t.Fatalf("delay(3)=%v outside expected jitter band", d3)
	}
}

func TestComputeDelayCapsAtMax(t *testing.T) {
	base := time.Second
	max := 5 * time.Second
	for failures := 1; failures < 20; failures++ {
		d := ComputeDelay(failures, base, max)
		if d > max {
			t.Fatalf("delay(%d)=%v exceeded cap %v", failures, d, max)
		}
	}
}

func TestComputeDelayClampsInvalidInputs(t *testing.T) {
	// failures <= 0 treated as 1.
	d := ComputeDelay(0, 0, 0)
	if d < 0 {
		t.Fatal("base delay must never be negative")
	}
	if d > 60*time.Second {
		t.Fatalf("default cap violated: %v", d)
	}
}
