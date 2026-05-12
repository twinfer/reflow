package engine

import (
	"math"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Default RunRetryPolicy values: matches Restate's defaults plus our
// "unbounded retries" convention (max_attempts=0 means no cap). The
// defaults are conservative for transient SDK-fn errors; production
// services override via StartInvocation.retry_policy. Phase 3.
const (
	defaultRetryInitialInterval = 50 * time.Millisecond
	defaultRetryFactor          = 2.0
	defaultRetryMaxInterval     = 10 * time.Second
)

// NextRetryDelay returns the wall-clock backoff for the (attempt+1)th
// retry given a policy. attempt is the zero-based count of attempts
// already executed and failed retryably; attempt=0 is "first failure
// just happened, wait this long before re-running fn".
//
// Returns (0, false) when the policy is exhausted (max_attempts reached).
// A nil policy or any zero/absent field is treated as the corresponding
// default; max_attempts=0 means unlimited.
//
// The math is exponential with a configurable factor, capped at
// max_interval. Overflows (NaN, ±Inf, negative) collapse to max_interval
// so the engine never schedules a non-firing or in-the-past timer.
func NextRetryDelay(p *enginev1.RunRetryPolicy, attempt uint32) (time.Duration, bool) {
	if p.GetMaxAttempts() > 0 && attempt >= p.GetMaxAttempts() {
		return 0, false
	}

	initial := time.Duration(p.GetInitialIntervalMs()) * time.Millisecond
	if initial <= 0 {
		initial = defaultRetryInitialInterval
	}
	factor := p.GetFactor()
	if factor <= 0 {
		factor = defaultRetryFactor
	}
	maxInterval := time.Duration(p.GetMaxIntervalMs()) * time.Millisecond
	if maxInterval <= 0 {
		maxInterval = defaultRetryMaxInterval
	}
	if initial > maxInterval {
		return maxInterval, true
	}

	raw := float64(initial) * math.Pow(factor, float64(attempt))
	if math.IsNaN(raw) || math.IsInf(raw, 0) || raw < 0 || raw > float64(maxInterval) {
		return maxInterval, true
	}
	return time.Duration(raw), true
}
