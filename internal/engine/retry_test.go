package engine

import (
	"testing"
	"time"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func TestNextRetryDelay_NilPolicyUsesDefaults(t *testing.T) {
	cases := []struct {
		attempt uint32
		want    time.Duration
	}{
		{0, 50 * time.Millisecond},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1600 * time.Millisecond},
		{6, 3200 * time.Millisecond},
		{7, 6400 * time.Millisecond},
		{8, 10 * time.Second}, // hits cap (12800ms > 10s)
		{20, 10 * time.Second},
	}
	for _, c := range cases {
		got, ok := NextRetryDelay(nil, c.attempt)
		if !ok {
			t.Errorf("attempt=%d: exhausted; want ok with default unlimited policy", c.attempt)
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
		MaxIntervalMs:     0, // default 10s
	}
	cases := []struct {
		attempt uint32
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 300 * time.Millisecond},
		{2, 900 * time.Millisecond},
		{3, 2700 * time.Millisecond},
		{4, 8100 * time.Millisecond},
		{5, 10 * time.Second}, // 24300ms > 10s cap
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
	p := &enginev1.RunRetryPolicy{MaxAttempts: 3}
	for _, attempt := range []uint32{0, 1, 2} {
		if _, ok := NextRetryDelay(p, attempt); !ok {
			t.Errorf("attempt=%d: exhausted; want ok (only 3 attempts allowed)", attempt)
		}
	}
	if d, ok := NextRetryDelay(p, 3); ok {
		t.Errorf("attempt=3 with max_attempts=3: got %v ok=true; want exhausted", d)
	}
	if d, ok := NextRetryDelay(p, 100); ok {
		t.Errorf("attempt=100: got %v ok=true; want exhausted", d)
	}
}

func TestNextRetryDelay_MaxAttemptsZeroIsUnlimited(t *testing.T) {
	p := &enginev1.RunRetryPolicy{MaxAttempts: 0}
	if _, ok := NextRetryDelay(p, 1_000_000); !ok {
		t.Errorf("max_attempts=0 should be unlimited; got exhausted at attempt=1_000_000")
	}
}

func TestNextRetryDelay_OverflowProtection(t *testing.T) {
	p := &enginev1.RunRetryPolicy{
		InitialIntervalMs: 1000,
		Factor:            10.0,
		MaxIntervalMs:     60_000, // 60s cap
	}
	// At very high attempt counts the float64 math overflows. Result must
	// still saturate at max_interval, never wrap negative or NaN out.
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
	}
	got, ok := NextRetryDelay(p, 0)
	if !ok {
		t.Fatal("exhausted unexpectedly")
	}
	if got != 5*time.Second {
		t.Errorf("got %v want 5s (initial above max → clamp)", got)
	}
}
