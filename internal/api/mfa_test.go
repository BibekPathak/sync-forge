package api

import (
	"testing"
)

func TestNewBackupCodes(t *testing.T) {
	raw, hashed, err := newBackupCodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != backupCodeCount || len(hashed) != backupCodeCount {
		t.Fatalf("expected %d codes, got %d raw %d hashed", backupCodeCount, len(raw), len(hashed))
	}
	// Format: XXXX-XXXX (9 chars, dash in the middle), uppercase base32 chars.
	for _, c := range raw {
		if len(c) != 9 || c[4] != '-' {
			t.Fatalf("unexpected code format: %q", c)
		}
		for _, r := range c {
			if r == '-' {
				continue
			}
			if !((r >= 'A' && r <= 'Z') || (r >= '2' && r <= '7')) {
				t.Fatalf("non-base32 char in code %q", c)
			}
		}
	}
	// Hashes match the raw codes and are unique.
	seen := map[string]bool{}
	for i := range raw {
		if backupCodeHash(raw[i]) != hashed[i] {
			t.Fatalf("hash mismatch for %q", raw[i])
		}
		if seen[raw[i]] {
			t.Fatalf("duplicate code %q", raw[i])
		}
		seen[raw[i]] = true
	}
}
