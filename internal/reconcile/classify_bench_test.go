package reconcile

import (
	"testing"

	"syncforge/internal/store"
)

// BenchmarkClassifyInSync measures the happy path: a live canonical record
// whose fields match the provider record exactly.
func BenchmarkClassifyInSync(b *testing.B) {
	canonical := &store.CanonicalRecord{
		Fields: map[string]any{
			"first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com",
			"phone": "+1-555-0101", "company": "Analytical Engines",
		},
	}
	provider := map[string]any{
		"first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com",
		"phone": "+1-555-0101", "company": "Analytical Engines",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if got := classify(canonical, provider); got != "" {
			b.Fatalf("expected in-sync, got %q", got)
		}
	}
}

// BenchmarkClassifyDrift measures the divergence path with differing fields.
func BenchmarkClassifyDrift(b *testing.B) {
	canonical := &store.CanonicalRecord{
		Fields: map[string]any{
			"first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com",
			"phone": "+1-555-0101", "company": "Analytical Engines",
		},
	}
	provider := map[string]any{
		"first_name": "Ada", "last_name": "Lovelace", "email": "ada@changed.com",
		"phone": "+1-555-0102", "company": "Analytical Engines",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if got := classify(canonical, provider); got != store.FindingDrift {
			b.Fatalf("expected drift, got %q", got)
		}
	}
}

// BenchmarkClassifyNumericCoercion measures the string-coercion path where
// provider values arrive as numbers and must compare equal to stored strings.
func BenchmarkClassifyNumericCoercion(b *testing.B) {
	canonical := &store.CanonicalRecord{
		Fields: map[string]any{
			"first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com",
			"phone": "5550101", "company": "Analytical Engines",
		},
	}
	provider := map[string]any{
		"first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com",
		"phone": 5550101, "company": "Analytical Engines",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if got := classify(canonical, provider); got != "" {
			b.Fatalf("expected coerced-equal in-sync, got %q", got)
		}
	}
}

// BenchmarkClassifyMissed measures the unknown-provider-id path.
func BenchmarkClassifyMissed(b *testing.B) {
	provider := map[string]any{
		"first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com",
		"phone": "+1-555-0101", "company": "Analytical Engines",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if got := classify(nil, provider); got != store.FindingMissed {
			b.Fatalf("expected missed, got %q", got)
		}
	}
}

// BenchmarkShouldRecreateMissing exercises the delete-policy gate.
func BenchmarkShouldRecreateMissing(b *testing.B) {
	policies := []string{"propagate", "ignore", "tombstone_only", ""}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = shouldRecreateMissing(policies[i%len(policies)])
	}
}
