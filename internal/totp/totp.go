// Package totp implements RFC 6238 time-based one-time passwords using
// HMAC-SHA1 (the algorithm behind most authenticator apps). It is a small,
// dependency-free alternative to pulling in an OTP library, and is unit-tested
// against the RFC's test vectors.
package totp

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base32"
	"encoding/binary"
	"io"
	"strings"
	"time"
)

// DefaultPeriod is the standard 30-second TOTP window.
const DefaultPeriod = 30 * time.Second

// NewSecret returns a random base32 secret (160 bits, no padding) suitable for
// enrolling an authenticator app.
func NewSecret() (string, error) {
	raw := make([]byte, 20)
	if _, err := randRead(raw); err != nil {
		return "", err
	}
	return strings.TrimRight(base32.StdEncoding.EncodeToString(raw), "="), nil
}

// Code computes the 6-digit TOTP code for secret at time t.
func Code(secret string, t time.Time) (string, error) {
	key, err := decodeSecret(secret)
	if err != nil {
		return "", err
	}
	return codeAt(key, t), nil
}

// Validate reports whether code is a correct 6-digit TOTP for secret at time t,
// allowing a one-window skew either side (clock drift tolerance).
func Validate(secret, code string, t time.Time) (bool, error) {
	key, err := decodeSecret(secret)
	if err != nil {
		return false, err
	}
	for _, offset := range []int{0, -1, 1} {
		want := codeAt(key, t.Add(time.Duration(offset)*DefaultPeriod))
		if constantTimeEqual(want, code) {
			return true, nil
		}
	}
	return false, nil
}

// URI renders an otpauth:// provisioning URI for QR display.
func URI(issuer, account, secret string) string {
	label := issuer + ":" + account
	return "otpauth://totp/" + label + "?secret=" + secret + "&issuer=" + issuer + "&algorithm=SHA1&digits=6&period=30"
}

func decodeSecret(secret string) ([]byte, error) {
	clean := strings.ToUpper(strings.TrimSpace(secret))
	// Authenticator apps store base32 without padding; re-add it for decoding.
	if pad := (8 - len(clean)%8) % 8; pad > 0 {
		clean += strings.Repeat("=", pad)
	}
	return base32.StdEncoding.DecodeString(clean)
}

func codeAt(key []byte, t time.Time) string {
	counter := uint64(t.Unix() / int64(DefaultPeriod.Seconds()))
	var msg [8]byte
	binary.BigEndian.PutUint64(msg[:], counter)

	mac := hmac.New(sha1.New, key)
	mac.Write(msg[:])
	sum := mac.Sum(nil)

	offset := sum[len(sum)-1] & 0x0f
	bin := (uint32(sum[offset])&0x7f)<<24 |
		uint32(sum[offset+1])<<16 |
		uint32(sum[offset+2])<<8 |
		uint32(sum[offset+3])
	code := bin % 1000000
	return pad6(code)
}

func pad6(n uint32) string {
	s := itoa(int(n))
	if len(s) >= 6 {
		return s
	}
	return strings.Repeat("0", 6-len(s)) + s
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func randRead(b []byte) (int, error) { return io.ReadFull(rand.Reader, b) }

func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
