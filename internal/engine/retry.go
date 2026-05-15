package engine

import (
	"math"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Default RunRetryPolicy values. The cap on attempts matters more than
// it might look: an uncapped stuck handler on a keyed object would
// poison the VO gate indefinitely — every subsequent invocation queued
// behind it would never run. 64 attempts covers genuine transient bursts
// (~9.3 min of wall-clock once the per-attempt delay saturates at
// max_interval) while guaranteeing eventual terminal-failure surfacing.
//
// A zero/absent max_attempts on a caller-supplied policy means "use
// this default", not "unlimited". Callers that genuinely want unbounded
// retries can set a very large value (e.g. math.MaxUint32).
const (
	defaultRetryInitialInterval = 50 * time.Millisecond
	defaultRetryFactor          = 2.0
	defaultRetryMaxInterval     = 10 * time.Second
	defaultRetryMaxAttempts     = 64
)

// NextRetryDelay returns the wall-clock backoff for the (attempt+1)th
// retry given a policy. attempt is the zero-based count of attempts
// already executed and failed retryably; attempt=0 is "first failure
// just happened, wait this long before re-running fn".
//
// Returns (0, false) when the policy is exhausted (attempt >= max).
// A nil policy or any zero/absent field is treated as the corresponding
// default; in particular, max_attempts=0 means "use defaultRetryMaxAttempts",
// not unlimited. Callers wanting unbounded retries must set max_attempts
// to a very large value (e.g. math.MaxUint32) explicitly.
//
// The math is exponential with a configurable factor, capped at
// max_interval. Overflows (NaN, ±Inf, negative) collapse to max_interval
// so the engine never schedules a non-firing or in-the-past timer.
func NextRetryDelay(p *enginev1.RunRetryPolicy, attempt uint32) (time.Duration, bool) {
	maxAttempts := p.GetMaxAttempts()
	if maxAttempts == 0 {
		maxAttempts = defaultRetryMaxAttempts
	}
	if attempt >= maxAttempts {
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
