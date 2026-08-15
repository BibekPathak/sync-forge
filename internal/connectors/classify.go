package connectors

import (
	"errors"
	"time"
)

// Classify inspects an error and returns its connector ErrorKind plus any
// RetryAfter hint (for rate limits). Unknown errors are classified UNKNOWN.
func Classify(err error) (ErrorKind, time.Duration) {
	var ce *Error
	if errors.As(err, &ce) {
		return ce.Kind, ce.RetryAfter
	}
	if err == nil {
		return "", 0
	}
	return ErrUnknown, 0
}

// ShouldRetry reports whether a failure of the given kind is worth retrying.
// Permanent failures (schema errors, auth failures, explicit permanent errors)
// go straight to the dead-letter queue instead of burning backoff time.
func ShouldRetry(kind ErrorKind) bool {
	switch kind {
	case ErrTransient, ErrRateLimited, ErrConflict, ErrNotFound, ErrUnknown:
		return true
	case ErrPermanent, ErrAuth, ErrSchema:
		return false
	}
	return true
}
