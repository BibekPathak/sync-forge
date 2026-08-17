package totp

import (
	"strings"
	"testing"
	"time"
)

// RFC 6238 Appendix B test vectors (SHA1, 8 digits in the RFC; we use 6).
// The documented 8-digit values for the given timestamps are:
//
//	59      -> 94287082
//	1111111109 -> 07081804
//	1111111111 -> 14050471
//	1234567890 -> 89005924
//	2000000000 -> 69279037
//
// We truncate to 6 digits (last 6 of each), which is the standard authenticator
// format.
func TestRFC6238Vectors(t *testing.T) {
	secret := "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ" // "12345678901234567890" base32
	cases := []struct {
		unix int64
		want string
	}{
		{59, "287082"},
		{1111111109, "081804"},
		{1111111111, "050471"},
		{1234567890, "005924"},
		{2000000000, "279037"},
	}
	for _, c := range cases {
		got, err := Code(secret, time.Unix(c.unix, 0).UTC())
		if err != nil {
			t.Fatalf("Code(%d): %v", c.unix, err)
		}
		if got != c.want {
			t.Errorf("Code(unix=%d) = %q, want %q", c.unix, got, c.want)
		}
	}
}

func TestValidate(t *testing.T) {
	secret, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	code, err := Code(secret, now)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := Validate(secret, code, now); !ok {
		t.Fatal("valid code rejected")
	}
	// Wrong code rejected.
	if ok, _ := Validate(secret, "000000", now); ok {
		t.Fatal("wrong code accepted")
	}
	// One-window skew (30s earlier/later) accepted.
	prev, err := Code(secret, now.Add(-30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := Validate(secret, prev, now); !ok {
		t.Fatal("one-window skew rejected")
	}
	// Far-off code rejected.
	old, err := Code(secret, now.Add(-5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := Validate(secret, old, now); ok {
		t.Fatal("five-minute-old code accepted")
	}
}

func TestNewSecretAndURI(t *testing.T) {
	secret, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	if len(secret) < 20 {
		t.Fatalf("secret too short: %q", secret)
	}
	// Base32 alphabet only.
	for _, r := range secret {
		if !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567", r) {
			t.Fatalf("non-base32 char in secret: %q", secret)
		}
	}
	uri := URI("SyncForge", "admin@acme.dev", secret)
	if !strings.HasPrefix(uri, "otpauth://totp/") || !strings.Contains(uri, "secret="+secret) {
		t.Fatalf("unexpected URI: %s", uri)
	}
}
