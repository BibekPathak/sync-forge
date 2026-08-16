package conflict

import (
	"testing"
	"time"
)

var benchTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// benchPersisted and benchFP model a canonical record where salesforce was the
// last writer of every field.
func benchPersisted() map[string]any {
	return map[string]any{
		"first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com",
		"phone": "+1-555-0101", "company": "Analytical Engines",
	}
}

func benchFP() FieldProvenance {
	return FieldProvenance{
		"first_name": {Source: "salesforce", Version: 3, OccurredAt: benchTime, Priority: 100},
		"last_name":  {Source: "salesforce", Version: 3, OccurredAt: benchTime, Priority: 100},
		"email":      {Source: "salesforce", Version: 3, OccurredAt: benchTime, Priority: 100},
		"phone":      {Source: "salesforce", Version: 3, OccurredAt: benchTime, Priority: 100},
		"company":    {Source: "salesforce", Version: 3, OccurredAt: benchTime, Priority: 100},
	}
}

// incoming conflicts on every field (hubspot, later).
func benchIncoming() map[string]any {
	return map[string]any{
		"first_name": "Ada", "last_name": "Hopper", "email": "ada@hubspot.com",
		"phone": "+1-555-0202", "company": "Grace Inc",
	}
}

// BenchmarkDetectNoConflicts measures the common case where the incoming
// source is also the last writer, so no field conflicts.
func BenchmarkDetectNoConflicts(b *testing.B) {
	persisted := benchPersisted()
	fp := benchFP()
	incoming := benchPersisted()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if cs := Detect(persisted, fp, incoming, "salesforce", 4, benchTime.Add(time.Second), 100); len(cs) != 0 {
			b.Fatalf("expected no conflicts, got %d", len(cs))
		}
	}
}

// BenchmarkDetectConflicts measures the concurrent-edit path.
func BenchmarkDetectConflicts(b *testing.B) {
	persisted := benchPersisted()
	fp := benchFP()
	incoming := benchIncoming()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if cs := Detect(persisted, fp, incoming, "hubspot", 1, benchTime.Add(2*time.Second), 200); len(cs) != 4 {
			b.Fatalf("expected 4 conflicts, got %d", len(cs))
		}
	}
}

// BenchmarkMergeNoConflict measures Merge when there is nothing to resolve.
func BenchmarkMergeNoConflict(b *testing.B) {
	persisted := benchPersisted()
	fp := benchFP()
	incoming := benchPersisted()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, cs, _ := Merge(StrategyFieldMerge, persisted, fp, incoming, "salesforce", 4, benchTime.Add(time.Second), 100)
		if len(cs) != 0 {
			b.Fatalf("expected no conflicts, got %d", len(cs))
		}
	}
}

// BenchmarkMergeWithConflict measures the field_merge auto-resolution path.
func BenchmarkMergeWithConflict(b *testing.B) {
	persisted := benchPersisted()
	fp := benchFP()
	incoming := benchIncoming()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, cs, _ := Merge(StrategyFieldMerge, persisted, fp, incoming, "hubspot", 1, benchTime.Add(2*time.Second), 200)
		if len(cs) == 0 {
			b.Fatal("expected conflicts")
		}
	}
}

// BenchmarkResolve exercises strategy selection per conflict.
func BenchmarkResolve(b *testing.B) {
	c := FieldConflict{
		Field: "email", SourceA: "salesforce", VersionA: 3,
		OccurredAtA: benchTime, PriorityA: 100, ValueA: "ada@example.com",
		SourceB: "hubspot", VersionB: 1, OccurredAtB: benchTime.Add(time.Second),
		PriorityB: 200, ValueB: "ada@hubspot.com",
	}
	strats := []string{StrategyLastWriteWins, StrategySourcePriority, StrategyFieldMerge, StrategyManual}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w := Resolve(strats[i%len(strats)], c)
		if w.Source == "" && strats[i%len(strats)] != StrategyManual {
			b.Fatal("expected a winner")
		}
	}
}
