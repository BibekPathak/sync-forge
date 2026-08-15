package conflict

import (
	"testing"
	"time"
)

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func p(source string, version int64, at time.Time, priority int) Provenance {
	return Provenance{Source: source, Version: version, OccurredAt: at, Priority: priority}
}

// TestDetectBasics covers the three branches of detection: same-source edits
// never conflict, different-source edits on changed values conflict, and
// re-asserting an identical value is not a conflict.
func TestDetectBasics(t *testing.T) {
	persisted := map[string]any{"first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com"}
	now := ts("2024-02-01T10:00:00Z")
	fp := FieldProvenance{
		"first_name": p("salesforce", 3, now, 100),
		"last_name":  p("hubspot", 2, now.Add(-time.Hour), 200),
		"email":      p("salesforce", 1, now.Add(-2*time.Hour), 100),
	}

	t.Run("same source rewrite does not conflict", func(t *testing.T) {
		in := map[string]any{"first_name": "Grace"}
		got := Detect(persisted, fp, in, "salesforce", 4, now, 100)
		if len(got) != 0 {
			t.Fatalf("expected no conflict, got %+v", got)
		}
	})

	t.Run("different source edits a field", func(t *testing.T) {
		in := map[string]any{"last_name": "Hopper"}
		got := Detect(persisted, fp, in, "salesforce", 4, now, 100)
		if len(got) != 1 || got[0].Field != "last_name" {
			t.Fatalf("expected conflict on last_name, got %+v", got)
		}
		if got[0].SourceA != "hubspot" || got[0].SourceB != "salesforce" {
			t.Fatalf("wrong sides: %+v", got[0])
		}
	})

	t.Run("re-asserting the same value is a no-op", func(t *testing.T) {
		in := map[string]any{"last_name": "Lovelace"}
		got := Detect(persisted, fp, in, "salesforce", 4, now, 100)
		if len(got) != 0 {
			t.Fatalf("expected no conflict for identical value, got %+v", got)
		}
	})

	t.Run("new field claimed by incoming source", func(t *testing.T) {
		in := map[string]any{"company": "Analytical Engines"}
		got := Detect(persisted, fp, in, "hubspot", 5, now, 200)
		if len(got) != 0 {
			t.Fatalf("expected no conflict for new field, got %+v", got)
		}
	})
}

// TestResolveStrategies pins the tie-breaking rules for each strategy.
func TestResolveStrategies(t *testing.T) {
	base := ts("2024-02-01T10:00:00Z")
	mk := func(atA time.Time, prioA int, atB time.Time, prioB int) FieldConflict {
		return FieldConflict{
			Field:   "phone",
			SourceA: "salesforce", VersionA: 3, OccurredAtA: atA, PriorityA: prioA, ValueA: "+1-111",
			SourceB: "hubspot", VersionB: 2, OccurredAtB: atB, PriorityB: prioB, ValueB: "+1-222",
		}
	}

	t.Run("last_write_wins picks later occurrence", func(t *testing.T) {
		w := Resolve(StrategyLastWriteWins, mk(base, 100, base.Add(time.Hour), 200))
		if w.Side != "b" {
			t.Fatalf("expected side b (later), got %s", w.Side)
		}
		w = Resolve(StrategyLastWriteWins, mk(base.Add(time.Hour), 100, base, 200))
		if w.Side != "a" {
			t.Fatalf("expected side a (later), got %s", w.Side)
		}
	})

	t.Run("last_write_wins tie breaks to higher priority", func(t *testing.T) {
		w := Resolve(StrategyLastWriteWins, mk(base, 200, base, 100))
		if w.Side != "b" {
			t.Fatalf("expected side b (lower priority number wins ties), got %s", w.Side)
		}
	})

	t.Run("source_priority picks lower priority number", func(t *testing.T) {
		w := Resolve(StrategySourcePriority, mk(base, 100, base.Add(time.Hour), 200))
		if w.Side != "a" {
			t.Fatalf("expected side a (higher priority), got %s", w.Side)
		}
		w = Resolve(StrategySourcePriority, mk(base.Add(time.Hour), 200, base, 100))
		if w.Side != "b" {
			t.Fatalf("expected side b (higher priority), got %s", w.Side)
		}
	})

	t.Run("source_priority tie breaks to later occurrence", func(t *testing.T) {
		w := Resolve(StrategySourcePriority, mk(base, 100, base.Add(time.Hour), 100))
		if w.Side != "b" {
			t.Fatalf("expected side b (equal priority, later), got %s", w.Side)
		}
	})

	t.Run("manual never auto-wins", func(t *testing.T) {
		w := Resolve(StrategyManual, mk(base, 100, base.Add(time.Hour), 200))
		if w.Side != "" {
			t.Fatalf("manual must not pick a winner, got side %s", w.Side)
		}
	})
}

// TestMerge reconciles a whole incoming event. Verify the merged field set,
// that non-conflicting fields move to the incoming writer, and that manual
// reports manual=true without mutating.
func TestMerge(t *testing.T) {
	now := ts("2024-02-01T10:00:00Z")
	persisted := map[string]any{"first_name": "Grace", "last_name": "Hopper", "email": "grace@example.com"}
	fp := FieldProvenance{
		"first_name": p("salesforce", 3, now.Add(-time.Hour), 100),
		"last_name":  p("salesforce", 3, now.Add(-time.Hour), 100),
		"email":      p("salesforce", 3, now.Add(-time.Hour), 100),
	}
	incoming := map[string]any{
		"first_name": "Grace",     // unchanged
		"last_name":  "Hoppertwo", // change
		"company":    "Navy",      // new field
	}

	// default merge strategy = last_write_wins; incoming is newer -> wins.
	fields, prov, detected, manual := Merge(StrategyLastWriteWins, persisted, fp, incoming, "hubspot", 4, now, 200)
	if manual {
		t.Fatal("last_write_wins must not be manual")
	}
	if len(detected) != 1 || detected[0].Field != "last_name" {
		t.Fatalf("expected 1 conflict on last_name, got %+v", detected)
	}
	if fields["last_name"] != "Hoppertwo" {
		t.Fatalf("expected last_name to resolve to incoming, got %v", fields["last_name"])
	}
	if fields["company"] != "Navy" {
		t.Fatalf("expected company adopted, got %v", fields["company"])
	}
	if fields["email"] != "grace@example.com" {
		t.Fatalf("lost untouched field email: %v", fields["email"])
	}
	if prov["last_name"].Source != "hubspot" {
		t.Fatalf("last_name ownership should move to hubspot, got %+v", prov["last_name"])
	}
	if prov["company"].Source != "hubspot" {
		t.Fatalf("company ownership should be hubspot, got %+v", prov["company"])
	}

	// manual strategy parks and mutates nothing.
	_, _, _, manual = Merge(StrategyManual, persisted, fp, incoming, "hubspot", 4, now, 200)
	if !manual {
		t.Fatal("manual must report manual=true")
	}
}

// TestFromToMap round-trips the jsonb representation.
func TestFromToMap(t *testing.T) {
	at := ts("2024-02-01T10:00:00Z")
	fp := FieldProvenance{"a": p("sf", 1, at, 100), "b": p("hs", 2, at, 200)}
	m := fp.ToMap()
	back := FromMap(m)
	if back["a"].Source != "sf" || back["b"].Version != 2 {
		t.Fatalf("round-trip mismatch: %+v", back)
	}
}
