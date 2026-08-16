package simulator

import (
	"sync"
	"time"
)

// FaultConfig drives failure injection on a simulated provider.
type FaultConfig struct {
	// FailureRate is the probability (0..1) that an API request fails with 500.
	FailureRate float64 `json:"failure_rate"`
	// LatencyMS adds artificial latency to every request.
	LatencyMS int `json:"latency_ms"`
	// AuthFailure makes the API return 401 for all requests.
	AuthFailure bool `json:"auth_failure"`
	// Malformed returns malformed JSON bodies instead of valid responses.
	Malformed bool `json:"malformed"`
	// RateLimitPerMin overrides the configured per-minute API limit. 0 = keep default.
	RateLimitPerMin int `json:"rate_limit_per_min"`
	// DropWebhooks suppresses webhook delivery entirely.
	DropWebhooks bool `json:"drop_webhooks"`
	// DuplicateWebhooks emits duplicate copies of every webhook.
	DuplicateWebhooks bool `json:"duplicate_webhooks"`
	// DuplicateWebhookCount is the number of extra copies to emit.
	DuplicateWebhookCount int `json:"duplicate_webhook_count"`
	// WebhookDelayMS delays each webhook dispatch.
	WebhookDelayMS int `json:"webhook_delay_ms"`
	// OutOfOrder delays webhooks with a random additional offset to break
	// arrival order.
	OutOfOrder bool `json:"out_of_order"`
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
	f.cfg = cfg
}

func (f *FaultManager) Get() FaultConfig {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cfg
}

// ShouldFail returns true with probability equal to the configured failure rate.
func (f *FaultManager) ShouldFail() bool {
	f.mu.RLock()
	rate := f.cfg.FailureRate
	f.mu.RUnlock()
	if rate <= 0 {
		return false
	}
	return f.rng.Float64() < rate
}

func (f *FaultManager) AuthFailure() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cfg.AuthFailure
}

func (f *FaultManager) Malformed() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cfg.Malformed
}

func (f *FaultManager) Latency() time.Duration {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return time.Duration(f.cfg.LatencyMS) * time.Millisecond
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

// WebhookOptions returns delivery mutation settings and how many total copies
// to send (1 + duplicates).
func (f *FaultManager) WebhookOptions() (drop, dup, outOfOrder bool, copies int, delay time.Duration, extraDelayMax time.Duration) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	c := f.cfg
	return c.DropWebhooks, c.DuplicateWebhooks, c.OutOfOrder, 1 + c.DuplicateWebhookCount,
		time.Duration(c.WebhookDelayMS) * time.Millisecond, time.Duration(c.WebhookDelayMS) * time.Millisecond
}
