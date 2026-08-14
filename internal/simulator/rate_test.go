package simulator

import (
	"testing"
	"time"
)

func TestRateLimiterBurst(t *testing.T) {
	rl := NewRateLimiter(5)
	for i := 0; i < 5; i++ {
		if ok, _ := rl.Allow(); !ok {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	if ok, _ := rl.Allow(); ok {
		t.Fatal("request beyond capacity should be rejected")
	}
}

func TestRateLimiterRefill(t *testing.T) {
	rl := NewRateLimiter(60)
	for i := 0; i < 60; i++ {
		rl.Allow()
	}
	ok, retryAfter := rl.Allow()
	if ok {
		t.Fatal("should be rejected after exhausting capacity")
	}
	if retryAfter <= 0 || retryAfter > 2*time.Second {
		t.Fatalf("unexpected retryAfter: %v", retryAfter)
	}

	time.Sleep(1100 * time.Millisecond)
	if ok, _ := rl.Allow(); !ok {
		t.Fatal("token should have refilled after ~1s")
	}
}

func TestRateLimiterSetCapacity(t *testing.T) {
	rl := NewRateLimiter(10)
	rl.SetCapacity(1)
	if ok, _ := rl.Allow(); !ok {
		t.Fatal("capacity 1 should allow one")
	}
	if ok, _ := rl.Allow(); ok {
		t.Fatal("should be rejected after single token used")
	}
}
