package simulator

import (
	"sync"
	"time"
)

// RateLimiter is a token bucket limiter used to simulate provider API rate
// limits (e.g. 100 requests/minute for Salesforce).
type RateLimiter struct {
	mu        sync.Mutex
	capacity  float64
	tokens    float64
	refillPer float64 // tokens per second
	last      time.Time
}

// NewRateLimiter creates a limiter allowing `perMinute` requests per minute
// with burst up to that same capacity.
func NewRateLimiter(perMinute int) *RateLimiter {
	return &RateLimiter{
		capacity:  float64(perMinute),
		tokens:    float64(perMinute),
		refillPer: float64(perMinute) / 60.0,
		last:      time.Now(),
	}
}

// Allow reports whether a request may proceed now, and how long the caller
// should wait if not. It is not perfectly fair under high concurrency, which
// is acceptable for a simulator.
func (r *RateLimiter) Allow() (ok bool, retryAfter time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(r.last).Seconds()
	if elapsed > 0 {
		r.tokens += elapsed * r.refillPer
		if r.tokens > r.capacity {
			r.tokens = r.capacity
		}
		r.last = now
	}

	if r.tokens >= 1 {
		r.tokens--
		return true, 0
	}
	wait := time.Duration((1 - r.tokens) / r.refillPer * float64(time.Second))
	return false, wait
}

// SetCapacity adjusts the limit at runtime (used by fault injection).
func (r *RateLimiter) SetCapacity(perMinute int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if perMinute > 0 {
		r.capacity = float64(perMinute)
		r.refillPer = float64(perMinute) / 60.0
		if r.tokens > r.capacity {
			r.tokens = r.capacity
		}
	}
}
