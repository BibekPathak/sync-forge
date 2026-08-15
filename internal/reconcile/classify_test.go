package reconcile

import (
	"testing"

	"syncforge/internal/store"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name           string
		canonical      *store.CanonicalRecord
		providerFields map[string]any
		want           string
	}{
		{
			name:           "unknown provider id",
			canonical:      nil,
			providerFields: map[string]any{"first_name": "Ada"},
			want:           store.FindingMissed,
		},
		{
			name: "tombstoned but live on provider",
			canonical: &store.CanonicalRecord{
				Tombstone: true,
				Fields:    map[string]any{"first_name": "Ada"},
			},
			providerFields: map[string]any{"first_name": "Ada"},
			want:           store.FindingDeleted,
		},
		{
			name: "in sync",
			canonical: &store.CanonicalRecord{
				Fields: map[string]any{"first_name": "Ada", "email": "ada@x.io"},
			},
			providerFields: map[string]any{"first_name": "Ada", "email": "ada@x.io"},
			want:           "",
		},
		{
			name: "coerced equal values are clean",
			canonical: &store.CanonicalRecord{
				Fields: map[string]any{"phone": "5550100"},
			},
			providerFields: map[string]any{"phone": int64(5550100)},
			want:           "",
		},
		{
			name: "drift with differing values",
			canonical: &store.CanonicalRecord{
				Fields: map[string]any{"phone": "5550100"},
			},
			providerFields: map[string]any{"phone": "5559999"},
			want:           store.FindingDrift,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classify(tc.canonical, tc.providerFields); got != tc.want {
				t.Errorf("classify() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestShouldRecreateMissing(t *testing.T) {
	for _, p := range []string{"ignore", "tombstone_only"} {
		if got := shouldRecreateMissing(p); !got {
			t.Errorf("shouldRecreateMissing(%q) = false, want true", p)
		}
	}
	for _, p := range []string{"propagate", ""} {
		if got := shouldRecreateMissing(p); got {
			t.Errorf("shouldRecreateMissing(%q) = true, want false", p)
		}
	}
}
