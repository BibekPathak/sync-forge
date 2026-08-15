package connectors

import (
	"context"
	"math"
	"sync"
	"time"
)

// Limiter is a token-bucket rate limiter used client-side to pace requests to
// an external provider. It prevents a single tenant from hammering a shared
// provider connector and smooths bursty backfills. A zero rate disables
// limiting.
type Limiter struct {
	mu       sync.Mutex
	rate     float64 // tokens per second
	capacity float64
	tokens   float64
	last     time.Time
}

// NewLimiter creates a token bucket with perMinute capacity and refill rate.
func NewLimiter(perMinute int) *Limiter {
	rate := float64(perMinute) / 60.0
	return &Limiter{
		rate:     rate,
		capacity: float64(perMinute),
		tokens:   float64(perMinute),
		last:     time.Now(),
	}
}

// Wait blocks until a token is available or ctx is cancelled. Safe for
// concurrent use.
func (l *Limiter) Wait(ctx context.Context) error {
	if l == nil || l.rate <= 0 {
		return nil
	}
	for {
		ok, wait := l.take(1)
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (l *Limiter) take(n float64) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.tokens = math.Min(l.capacity, l.tokens+now.Sub(l.last).Seconds()*l.rate)
	l.last = now
	if l.tokens >= n {
		l.tokens -= n
		return true, 0
	}
	need := n - l.tokens
	return false, time.Duration(need / l.rate * float64(time.Second))
}
