// Package conflict implements field-level conflict detection and resolution
// for concurrent edits across synchronized systems. It is a pure leaf
// package: no database or worker dependencies, so the detection and
// resolution rules are unit-testable in isolation.
//
// Provenance model: canonical_records.field_provenance is a jsonb map
//
//	{"field": {"source": "...", "version": N, "occurred_at": "...", "priority": N}}
//
// recording which source last wrote each field. A conflict arises when an
// incoming event changes a field whose last writer is a *different* source
// (a concurrent edit), or re-asserts a value a different source already
// overwrote.
package conflict

import (
	"encoding/json"
	"reflect"
	"sort"
	"time"
)

// Strategy names, mirrored from sync_policies.conflict_strategy.
const (
	StrategyLastWriteWins  = "last_write_wins"
	StrategySourcePriority = "source_priority"
	StrategyFieldMerge     = "field_merge"
	StrategyManual         = "manual"
)

// Provenance identifies which source last wrote a field, the version it was
// at that write, and the source's configured priority (used by
// source_priority resolution).
type Provenance struct {
	Source     string    `json:"source"`
	Version    int64     `json:"version"`
	OccurredAt time.Time `json:"occurred_at"`
	Priority   int       `json:"priority,omitempty"`
}

// FieldProvenance maps canonical field names to their last writer.
type FieldProvenance map[string]Provenance

// FieldConflict is one field whose value was edited concurrently by two
// different sources (or whose value differs from what the last writer left).
type FieldConflict struct {
	Field string

	// SourceA is the already-applied side (the last writer per provenance);
	// SourceB is the incoming event's side.
	SourceA     string
	VersionA    int64
	OccurredAtA time.Time
	PriorityA   int
	ValueA      any

	SourceB     string
	VersionB    int64
	OccurredAtB time.Time
	PriorityB   int
	ValueB      any
}

// Detect returns the fields that actually conflict between the persisted
// canonical fields/provenance and an incoming event. A field conflicts when
// the incoming value differs from the stored value AND the stored value was
// last written by a different source. Fields the incoming source itself last
// wrote (its own ongoing edits) never conflict.
func Detect(persisted map[string]any, fp FieldProvenance, incoming map[string]any, source string, version int64, occurredAt time.Time, priority int) []FieldConflict {
	var out []FieldConflict
	for field, inVal := range incoming {
		prov, ok := fp[field]
		if !ok {
			continue // field never written before: incoming source claims it
		}
		if prov.Source == source {
			continue // same source still editing: ordering rules apply, not conflicts
		}
		stored := persisted[field]
		if valuesEqual(stored, inVal) {
			continue // no effective difference: nothing to battle over
		}
		out = append(out, FieldConflict{
			Field:       field,
			SourceA:     prov.Source,
			VersionA:    prov.Version,
			OccurredAtA: prov.OccurredAt,
			PriorityA:   prov.Priority,
			ValueA:      stored,
			SourceB:     source,
			VersionB:    version,
			OccurredAtB: occurredAt,
			PriorityB:   priority,
			ValueB:      inVal,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Field < out[j].Field })
	return out
}

// Winner indicates which side of a conflict wins under the active strategy.
type Winner struct {
	// Side is "a" (the last writer's value) or "b" (the incoming value).
	// Manual resolution always reports Side "" because it never auto-wins.
	Side       string
	Source     string
	Version    int64
	OccurredAt time.Time
	Value      any
}

// Resolve applies a strategy to a single conflicted field and returns which
// side wins. last_write_wins and field_merge pick the later OccurredAt (ties
// go to the higher-priority source = lower priority number, then to the
// existing side). source_priority picks the lower priority number, breaking
// ties by occurrence time. manual never picks a winner.
func Resolve(strategy string, c FieldConflict) Winner {
	switch strategy {
	case StrategyLastWriteWins, StrategyFieldMerge:
		switch {
		case c.OccurredAtB.After(c.OccurredAtA):
			return winnerB(c)
		case c.OccurredAtB.Before(c.OccurredAtA):
			return winnerA(c)
		default: // simultaneous: higher-priority source wins, else existing
			if c.PriorityB < c.PriorityA {
				return winnerB(c)
			}
			return winnerA(c)
		}
	case StrategySourcePriority:
		switch {
		case c.PriorityB < c.PriorityA:
			return winnerB(c)
		case c.PriorityB > c.PriorityA:
			return winnerA(c)
		default: // equal priority: later occurrence, else existing
			if c.OccurredAtB.After(c.OccurredAtA) {
				return winnerB(c)
			}
			return winnerA(c)
		}
	case StrategyManual:
		return Winner{}
	default:
		return winnerA(c)
	}
}

func winnerA(c FieldConflict) Winner {
	return Winner{Side: "a", Source: c.SourceA, Version: c.VersionA, OccurredAt: c.OccurredAtA, Value: c.ValueA}
}

func winnerB(c FieldConflict) Winner {
	return Winner{Side: "b", Source: c.SourceB, Version: c.VersionB, OccurredAt: c.OccurredAtB, Value: c.ValueB}
}

// Merge applies a strategy across a whole incoming event against the persisted
// canonical state. For auto strategies it returns the resolved field set
// (winner values per field) plus the updated provenance. For manual it leaves
// fields/provenance untouched and reports manual=true so the caller can park
// the conflict instead of applying.
func Merge(strategy string, persisted map[string]any, fp FieldProvenance, incoming map[string]any, source string, version int64, occurredAt time.Time, priority int) (map[string]any, FieldProvenance, []FieldConflict, bool) {
	detected := Detect(persisted, fp, incoming, source, version, occurredAt, priority)
	if len(detected) == 0 {
		// No fight: the incoming source simply claims the incoming fields.
		out := cloneMap(persisted)
		prov := cloneProvenance(fp)
		for field, v := range incoming {
			out[field] = v
			prov[field] = Provenance{Source: source, Version: version, OccurredAt: occurredAt, Priority: priority}
		}
		return out, prov, nil, false
	}

	strategy = normalizeStrategy(strategy)
	if strategy == StrategyManual {
		return nil, nil, detected, true
	}

	out := cloneMap(persisted)
	prov := cloneProvenance(fp)
	for _, c := range detected {
		w := Resolve(strategy, c)
		out[c.Field] = w.Value
		prov[c.Field] = Provenance{Source: w.Source, Version: w.Version, OccurredAt: w.OccurredAt, Priority: priorityFor(w.Source, c)}
	}
	// Any incoming field that did not conflict is simply adopted by its source.
	for field, v := range incoming {
		if _, ok := find(detected, field); ok {
			continue
		}
		out[field] = v
		prov[field] = Provenance{Source: source, Version: version, OccurredAt: occurredAt, Priority: priority}
	}
	return out, prov, detected, false
}

func normalizeStrategy(s string) string {
	switch s {
	case StrategyLastWriteWins, StrategySourcePriority, StrategyFieldMerge, StrategyManual:
		return s
	default:
		return StrategyLastWriteWins
	}
}

func priorityFor(source string, c FieldConflict) int {
	if source == c.SourceA {
		return c.PriorityA
	}
	return c.PriorityB
}

func find(fs []FieldConflict, field string) (FieldConflict, bool) {
	for _, f := range fs {
		if f.Field == field {
			return f, true
		}
	}
	return FieldConflict{}, false
}

// FromMap decodes the persisted jsonb provenance into a typed map.
func FromMap(m map[string]any) FieldProvenance {
	fp := FieldProvenance{}
	for k, v := range m {
		raw, _ := json.Marshal(v)
		var p Provenance
		_ = json.Unmarshal(raw, &p)
		fp[k] = p
	}
	return fp
}

// ToMap encodes the typed provenance for jsonb persistence.
func (fp FieldProvenance) ToMap() map[string]any {
	out := make(map[string]any, len(fp))
	for k, v := range fp {
		out[k] = v
	}
	return out
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneProvenance(fp FieldProvenance) FieldProvenance {
	out := make(FieldProvenance, len(fp))
	for k, v := range fp {
		out[k] = v
	}
	return out
}

// valuesEqual compares two json-decoded values (which may be float64 vs int,
// or []any vs slices) canonically.
func valuesEqual(a, b any) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return reflect.DeepEqual(ja, jb)
}
