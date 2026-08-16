package connectors

import (
	"errors"
	"testing"
)

var (
	benchTransient = NewError(ErrTransient, "upstream timeout", errors.New("dial tcp: i/o timeout"))
	benchRateLimit = NewError(ErrRateLimited, "throttled", nil)
	benchAuth      = NewError(ErrAuth, "invalid token", nil)
	benchSchema    = NewError(ErrSchema, "unexpected field", nil)
	benchPlain     = errors.New("generic go error")
)

// BenchmarkClassifyTyped measures classification of a typed connector error.
func BenchmarkClassifyTyped(b *testing.B) {
	errs := []error{benchTransient, benchRateLimit, benchAuth, benchSchema}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kind, _ := Classify(errs[i%len(errs)])
		if kind == "" {
			b.Fatal("expected a kind")
		}
	}
}

// BenchmarkClassifyUntyped measures the fallback path for a plain error.
func BenchmarkClassifyUntyped(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kind, _ := Classify(benchPlain)
		if kind != ErrUnknown {
			b.Fatalf("expected UNKNOWN, got %q", kind)
		}
	}
}

// BenchmarkClassifyNil measures the nil-error path.
func BenchmarkClassifyNil(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kind, _ := Classify(nil)
		if kind != "" {
			b.Fatalf("expected empty kind, got %q", kind)
		}
	}
}

// BenchmarkShouldRetry measures the retry decision across kinds.
func BenchmarkShouldRetry(b *testing.B) {
	kinds := []ErrorKind{ErrTransient, ErrRateLimited, ErrPermanent, ErrAuth, ErrSchema, ErrConflict, ErrNotFound}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ShouldRetry(kinds[i%len(kinds)])
	}
}
