package engine

import (
	"testing"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func TestNextRetryDelay_NilPolicyExhaustsImmediately(t *testing.T) {
	// Default max_attempts is 1 — first failure exhausts the policy.
	// Retry is opt-in per call via RunRetryPolicy.max_attempts > 1.
	if d, ok := NextRetryDelay(nil, 1); ok {
		t.Errorf("attempt=1 with default policy: got %v ok=true; want exhausted", d)
	}
	if d, ok := NextRetryDelay(nil, 2); ok {
		t.Errorf("attempt=2: got %v ok=true; want exhausted", d)
	}
}

func TestNextRetryDelay_BackoffSchedule(t *testing.T) {
	// With an explicit larger cap, verify the exponential schedule.
	p := &enginev1.RunRetryPolicy{MaxAttempts: 1 << 10}
	cases := []struct {
		attempt uint32
		want    time.Duration
	}{
		{1, 50 * time.Millisecond},
		{2, 100 * time.Millisecond},
		{3, 200 * time.Millisecond},
		{4, 400 * time.Millisecond},
		{5, 800 * time.Millisecond},
		{6, 1600 * time.Millisecond},
		{7, 3200 * time.Millisecond},
		{8, 6400 * time.Millisecond},
		{9, 10 * time.Second}, // hits cap (12800ms > 10s)
		{20, 10 * time.Second},
	}
	for _, c := range cases {
		got, ok := NextRetryDelay(p, c.attempt)
		if !ok {
			t.Errorf("attempt=%d: exhausted; want ok within cap", c.attempt)
		}
		if got != c.want {
			t.Errorf("attempt=%d: got %v want %v", c.attempt, got, c.want)
		}
	}
}

func TestNextRetryDelay_CustomFactor(t *testing.T) {
	p := &enginev1.RunRetryPolicy{
		InitialIntervalMs: 100,
		Factor:            3.0,
		MaxIntervalMs:     0,    // default 10s
		MaxAttempts:       1024, // generous so we exercise the schedule
	}
	cases := []struct {
		attempt uint32
		want    time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 300 * time.Millisecond},
		{3, 900 * time.Millisecond},
		{4, 2700 * time.Millisecond},
		{5, 8100 * time.Millisecond},
		{6, 10 * time.Second}, // 24300ms > 10s cap
	}
	for _, c := range cases {
		got, ok := NextRetryDelay(p, c.attempt)
		if !ok {
			t.Errorf("attempt=%d: exhausted; want ok", c.attempt)
		}
		if got != c.want {
			t.Errorf("attempt=%d: got %v want %v", c.attempt, got, c.want)
		}
	}
}

func TestNextRetryDelay_MaxAttemptsExhausts(t *testing.T) {
	// MaxAttempts=3 means 3 total fn invocations: initial + 2 retries.
	// After the 3rd attempt fails the policy is exhausted.
	p := &enginev1.RunRetryPolicy{MaxAttempts: 3}
	for _, attempt := range []uint32{1, 2} {
		if _, ok := NextRetryDelay(p, attempt); !ok {
			t.Errorf("attempt=%d: exhausted; want ok (max_attempts=3 permits 2 retries)", attempt)
		}
	}
	if d, ok := NextRetryDelay(p, 3); ok {
		t.Errorf("attempt=3 with max_attempts=3: got %v ok=true; want exhausted", d)
	}
	if d, ok := NextRetryDelay(p, 100); ok {
		t.Errorf("attempt=100: got %v ok=true; want exhausted", d)
	}
}

func TestNextRetryDelay_MaxAttemptsOneNoRetry(t *testing.T) {
	// MaxAttempts=1 means no retry — first failure is terminal.
	p := &enginev1.RunRetryPolicy{MaxAttempts: 1}
	if d, ok := NextRetryDelay(p, 1); ok {
		t.Errorf("max_attempts=1 first failure: got %v ok=true; want exhausted", d)
	}
}

func TestNextRetryDelay_MaxAttemptsZeroUsesDefault(t *testing.T) {
	// max_attempts=0 on a caller-supplied policy means "use the default",
	// not "unlimited". Same exhaustion behavior as a nil policy.
	p := &enginev1.RunRetryPolicy{MaxAttempts: 0}
	if _, ok := NextRetryDelay(p, 1); ok {
		t.Errorf("max_attempts=0 should match default (1) and exhaust at attempt=1")
	}
}

func TestNextRetryDelay_ExplicitlyLargeMaxAttemptsAllowsUnbounded(t *testing.T) {
	p := &enginev1.RunRetryPolicy{MaxAttempts: 1 << 30}
	if _, ok := NextRetryDelay(p, 1_000_000); !ok {
		t.Errorf("explicit large max_attempts should permit attempt=1_000_000")
	}
}

func TestNextRetryDelay_OverflowProtection(t *testing.T) {
	p := &enginev1.RunRetryPolicy{
		InitialIntervalMs: 1000,
		Factor:            10.0,
		MaxIntervalMs:     60_000,
		MaxAttempts:       1 << 30,
	}
	for _, attempt := range []uint32{50, 200, 1_000_000} {
		got, ok := NextRetryDelay(p, attempt)
		if !ok {
			t.Errorf("attempt=%d: exhausted unexpectedly", attempt)
		}
		if got != 60*time.Second {
			t.Errorf("attempt=%d: got %v want 60s cap", attempt, got)
		}
	}
}

func TestNextRetryDelay_InitialAboveMaxClampsImmediately(t *testing.T) {
	p := &enginev1.RunRetryPolicy{
		InitialIntervalMs: 30_000,
		Factor:            2.0,
		MaxIntervalMs:     5_000,
		MaxAttempts:       1024,
	}
	got, ok := NextRetryDelay(p, 1)
	if !ok {
		t.Fatal("exhausted unexpectedly")
	}
	if got != 5*time.Second {
		t.Errorf("got %v want 5s (initial above max → clamp)", got)
	}
}
