package connectors

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestShouldRetry(t *testing.T) {
	yes := []ErrorKind{ErrTransient, ErrRateLimited, ErrConflict, ErrNotFound, ErrUnknown}
	for _, k := range yes {
		if !ShouldRetry(k) {
			t.Fatalf("expected %s to be retryable", k)
		}
	}
	no := []ErrorKind{ErrPermanent, ErrAuth, ErrSchema}
	for _, k := range no {
		if ShouldRetry(k) {
			t.Fatalf("expected %s to be non-retryable", k)
		}
	}
}

func TestClassifyTypedError(t *testing.T) {
	ce := &Error{Kind: ErrRateLimited, Message: "slow down", RetryAfter: 42 * time.Second}
	kind, after := Classify(ce)
	if kind != ErrRateLimited {
		t.Fatalf("kind=%s want RATE_LIMITED", kind)
	}
	if after != 42*time.Second {
		t.Fatalf("retryAfter=%v want 42s", after)
	}
}

func TestClassifyWrappedAndUnknown(t *testing.T) {
	// A wrapped typed error still classifies.
	wrapped := fmt.Errorf("outer: %w", &Error{Kind: ErrAuth, Message: "denied"})
	if kind, _ := Classify(wrapped); kind != ErrAuth {
		t.Fatalf("wrapped classify kind=%s want AUTHENTICATION", kind)
	}

	// Plain errors classify as UNKNOWN.
	if kind, _ := Classify(errors.New("boom")); kind != ErrUnknown {
		t.Fatalf("plain error classify kind=%s want UNKNOWN", kind)
	}
	// nil.
	if kind, _ := Classify(nil); kind != "" {
		t.Fatalf("nil classify kind=%q want empty", kind)
	}
}
