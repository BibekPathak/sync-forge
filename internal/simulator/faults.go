package simulator

import (
	"sync"
	"time"
)

// FaultConfig drives failure injection on a simulated provider. Boolean
// fields force a fault always; the *Rate probability fields (0..1) apply the
// fault to that fraction of requests/webhooks using a deterministic seed, so a
// scenario is reproducible (and randomized chaos is available by changing the
// seed).
type FaultConfig struct {
	// Seed overrides the RNG seed for this config (0 = keep the manager default
	// of 42). Same seed -> same injected fault sequence.
	Seed int64 `json:"seed,omitempty"`
	// FailureRate is the probability (0..1) that an API request fails with 500.
	FailureRate float64 `json:"failure_rate"`
	// LatencyMS adds artificial latency to every request.
	LatencyMS int `json:"latency_ms"`
	// LatencyMinMS/LatencyMaxMS inject latency as a uniform range. When both
	// are >0 they override LatencyMS.
	LatencyMinMS int `json:"latency_min_ms"`
	LatencyMaxMS int `json:"latency_max_ms"`
	// AuthFailure makes the API return 401 for all requests; AuthFailureRate
	// applies it to a fraction of requests.
	AuthFailure bool `json:"auth_failure"`
	// AuthFailureRate applies auth failures to a fraction (0..1) of requests.
	AuthFailureRate float64 `json:"auth_failure_rate"`
	// Malformed returns malformed JSON bodies instead of valid responses;
	// MalformedRate applies it to a fraction of responses.
	Malformed bool `json:"malformed"`
	// MalformedRate applies malformed responses to a fraction (0..1).
	MalformedRate float64 `json:"malformed_rate"`
	// RateLimitPerMin overrides the configured per-minute API limit. 0 = keep default.
	RateLimitPerMin int `json:"rate_limit_per_min"`
	// RateLimitProbability applies the rate limit to a fraction (0..1) of
	// requests (unlimited otherwise).
	RateLimitProbability float64 `json:"rate_limit_probability"`
	// DropWebhooks suppresses webhook delivery entirely; DropWebhookRate applies
	// it to a fraction of webhooks.
	DropWebhooks bool `json:"drop_webhooks"`
	// DropWebhookRate drops a fraction (0..1) of webhooks.
	DropWebhookRate float64 `json:"drop_webhook_rate"`
	// DuplicateWebhooks emits duplicate copies of every webhook; DuplicateWebhookRate
	// duplicates a fraction of webhooks.
	DuplicateWebhooks bool `json:"duplicate_webhooks"`
	// DuplicateWebhookCount is the number of extra copies to emit.
	DuplicateWebhookCount int `json:"duplicate_webhook_count"`
	// DuplicateWebhookRate duplicates a fraction (0..1) of webhooks.
	DuplicateWebhookRate float64 `json:"duplicate_webhook_rate"`
	// WebhookDelayMS delays each webhook dispatch.
	WebhookDelayMS int `json:"webhook_delay_ms"`
	// OutOfOrder delays webhooks with a random additional offset to break
	// arrival order; OutOfOrderRate applies it to a fraction of webhooks.
	OutOfOrder bool `json:"out_of_order"`
	// OutOfOrderRate applies out-of-order delivery to a fraction (0..1).
	OutOfOrderRate float64 `json:"out_of_order_rate"`
	// HangMS sleeps for this duration before responding, simulating a hung
	// provider. When HangPercent is set, only that fraction of requests hang.
	HangMS int `json:"hang_ms"`
	// HangPercent is the probability (0..1) that a request hangs for HangMS.
	HangPercent float64 `json:"hang_percent"`
	// DropField removes the named field from list/get responses, simulating
	// partial payload corruption that passes JSON parsing but fails schema
	// validation.
	DropField string `json:"drop_field"`
	// CorruptFieldType sets the named field to a wrong-typed value (a nested
	// object) in list/get responses, simulating type corruption.
	CorruptFieldType string `json:"corrupt_field_type"`
}

func defaultFaults() FaultConfig {
	return FaultConfig{DuplicateWebhookCount: 0}
}

// FaultManager holds the current fault configuration and applies it to
// requests and webhook delivery.
type FaultManager struct {
	mu  sync.RWMutex
	cfg FaultConfig
	rng *safeRand
}

func NewFaultManager() *FaultManager {
	return &FaultManager{cfg: defaultFaults(), rng: newSafeRand()}
}

func (f *FaultManager) Set(cfg FaultConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cfg.Seed != 0 {
		f.rng.Seed(cfg.Seed)
	}
	f.cfg = cfg
}

func (f *FaultManager) Get() FaultConfig {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cfg
}

// should reports whether an event fires with the given probability. A
// probability <=0 never fires; >=1 always fires.
func (f *FaultManager) should(rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	return f.rng.Float64() < rate
}

// ShouldFail returns true with probability equal to the configured failure rate.
func (f *FaultManager) ShouldFail() bool {
	f.mu.RLock()
	rate := f.cfg.FailureRate
	f.mu.RUnlock()
	return f.should(rate)
}

func (f *FaultManager) AuthFailure() bool {
	f.mu.RLock()
	forced, rate := f.cfg.AuthFailure, f.cfg.AuthFailureRate
	f.mu.RUnlock()
	return forced || f.should(rate)
}

func (f *FaultManager) Malformed() bool {
	f.mu.RLock()
	forced, rate := f.cfg.Malformed, f.cfg.MalformedRate
	f.mu.RUnlock()
	return forced || f.should(rate)
}

func (f *FaultManager) Latency() time.Duration {
	f.mu.RLock()
	min, max := f.cfg.LatencyMinMS, f.cfg.LatencyMaxMS
	fixed := f.cfg.LatencyMS
	f.mu.RUnlock()
	if min > 0 && max >= min {
		return time.Duration(min+f.rng.Intn(max-min+1)) * time.Millisecond
	}
	return time.Duration(fixed) * time.Millisecond
}

// Hang reports whether the current request should hang (with probability
// HangPercent) and for how long.
func (f *FaultManager) Hang() (bool, time.Duration) {
	f.mu.RLock()
	ms := f.cfg.HangMS
	pct := f.cfg.HangPercent
	f.mu.RUnlock()
	if ms <= 0 {
		return false, 0
	}
	if pct > 0 && f.rng.Float64() >= pct {
		return false, 0
	}
	return true, time.Duration(ms) * time.Millisecond
}

// CorruptField returns the field to drop from list/get responses, if any.
func (f *FaultManager) CorruptField() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cfg.DropField
}

// CorruptFieldType returns the field to corrupt to a wrong type, if any.
func (f *FaultManager) CorruptFieldType() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cfg.CorruptFieldType
}

func (f *FaultManager) RateLimitPerMin() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cfg.RateLimitPerMin
}

// RateLimited reports whether the current request is subject to the rate
// limit. RateLimitProbability 0 means "always apply when a limit is set";
// >0 applies the limit to only that fraction of requests.
func (f *FaultManager) RateLimited() bool {
	f.mu.RLock()
	prob := f.cfg.RateLimitProbability
	hasLimit := f.cfg.RateLimitPerMin > 0
	f.mu.RUnlock()
	if !hasLimit {
		return false
	}
	if prob <= 0 {
		return true // 0 probability means always (backward compatible)
	}
	return f.should(prob)
}

// WebhookOptions returns delivery mutation settings and how many total copies
// to send (1 + duplicates). Boolean flags force the fault; the *Rate fields
// apply it per-webhook using the deterministic RNG.
func (f *FaultManager) WebhookOptions() (drop, dup, outOfOrder bool, copies int, delay time.Duration, extraDelayMax time.Duration) {
	f.mu.RLock()
	c := f.cfg
	f.mu.RUnlock()
	drop = c.DropWebhooks || f.should(c.DropWebhookRate)
	dup = c.DuplicateWebhooks || f.should(c.DuplicateWebhookRate)
	outOfOrder = c.OutOfOrder || f.should(c.OutOfOrderRate)
	copies = 1
	if dup {
		copies += c.DuplicateWebhookCount
	}
	delay = time.Duration(c.WebhookDelayMS) * time.Millisecond
	return drop, dup, outOfOrder, copies, delay, delay
}
