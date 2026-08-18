//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"syncforge/internal/store"
)

// TestLoginLockout proves repeated failures lock an account: after the
// configured failure count, even a correct password is rejected with 429 until
// the failure history is cleared.
func TestLoginLockout(t *testing.T) {
	ts, srv := newAPIServer(t)

	// Fail N times with a wrong password.
	for i := 0; i < 5; i++ {
		if _, status := loginAs(t, ts, "admin@acme.dev", "wrong-password"); status != http.StatusUnauthorized {
			t.Fatalf("attempt %d: expected 401, got %d", i+1, status)
		}
	}

	// The correct password is now rejected: account locked.
	if _, status := loginAs(t, ts, "admin@acme.dev", "syncforge-demo"); status != http.StatusTooManyRequests {
		t.Fatalf("locked account with correct password: expected 429, got %d", status)
	}

	// The failure history is durable in login_attempts.
	tenantID := tenantIDFor(t, srv, "acme")
	n, err := store.CountRecentFailures(context.Background(), srv.DB().Admin, tenantID, "admin@acme.dev", 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n < 5 {
		t.Fatalf("expected >=5 recorded failures, got %d", n)
	}

	// Clearing the failure history unlocks the account.
	if err := store.ClearLoginFailures(context.Background(), srv.DB().Admin, tenantID, "admin@acme.dev"); err != nil {
		t.Fatal(err)
	}
	if _, status := loginAs(t, ts, "admin@acme.dev", "syncforge-demo"); status != http.StatusOK {
		t.Fatalf("login after clearing lockout: expected 200, got %d", status)
	}
}

// TestLoginSuccessResetsFailureCounter proves a successful login clears the
// failure history, so subsequent failures start counting from zero.
func TestLoginSuccessResetsFailureCounter(t *testing.T) {
	ts, srv := newAPIServer(t)

	// Two failures, then a success.
	if _, status := loginAs(t, ts, "admin@acme.dev", "wrong-password"); status != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", status)
	}
	if _, status := loginAs(t, ts, "admin@acme.dev", "wrong-password"); status != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", status)
	}
	if _, status := loginAs(t, ts, "admin@acme.dev", "syncforge-demo"); status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	// Failure history is gone after success.
	tenantID := tenantIDFor(t, srv, "acme")
	n, err := store.CountRecentFailures(context.Background(), srv.DB().Admin, tenantID, "admin@acme.dev", 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected failure history cleared after success, got %d", n)
	}
}
