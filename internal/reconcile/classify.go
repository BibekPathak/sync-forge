// Package reconcile implements Phase 6 reconciliation: it walks a provider's
// records, compares them against the canonical model, classifies every
// divergence, and either repairs it immediately (auto mode) or parks it as a
// finding for operator review (manual mode).
package reconcile

import (
	"fmt"

	"syncforge/internal/store"
)

// classify compares a provider record's normalized fields against the
// canonical model and returns the reconciliation kind, or "" when the record is
// in sync. canonical is nil when the provider id is unknown to SyncForge.
func classify(canonical *store.CanonicalRecord, providerFields map[string]any) string {
	if canonical == nil {
		// Provider has a record we never ingested.
		return store.FindingMissed
	}
	if canonical.Tombstone {
		// Canonical says deleted but the provider still serves a live record.
		return store.FindingDeleted
	}
	if !sameFields(canonical.Fields, providerFields) {
		// Live canonical, live provider record, differing state.
		return store.FindingDrift
	}
	return ""
}

// sameFields reports whether two canonical-shaped field maps agree on every
// known customer field. Values are string-coerced so provider adapters that
// return numbers and SyncForge's persisted strings compare equal.
func sameFields(a, b map[string]any) bool {
	for _, k := range []string{"first_name", "last_name", "email", "phone", "company"} {
		if fmt.Sprint(a[k]) != fmt.Sprint(b[k]) {
			return false
		}
	}
	return true
}

// shouldRecreateMissing reports whether an auto-mode run may recreate a
// canonical record that a provider no longer has. Recreating resurrects data a
// provider deleted out-of-band, so it is only permitted when the tenant's
// delete policy treats deletions as non-propagating: 'tombstone_only' (we
// tombstone locally but never forward the deletion) or 'ignore' (external
// deletions are ignored entirely). Under 'propagate' an external delete is a
// deliberate signal and must not be undone, so the divergence is parked for an
// operator instead.
func shouldRecreateMissing(deletePolicy string) bool {
	return deletePolicy == "tombstone_only" || deletePolicy == "ignore"
}
