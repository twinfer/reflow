package reflow

import "testing"

// TestWithDefaults_MinSecondsBetweenTransfers_Pointer covers the
// koanf zero-vs-unset distinction at the withDefaults layer. The
// pointer type lets withDefaults tell "operator wants no cooldown"
// (non-nil pointer to 0) from "operator didn't say" (nil pointer);
// only the latter gets the production default 60s.
//
// The koanf decoder side — verifying that a YAML
// `min_seconds_between_transfers: 0` lands as a non-nil pointer —
// lives in pkg/reflow/config so the test file stays out of the
// pkg/reflow ↔ pkg/reflow/config import cycle.
func TestWithDefaults_MinSecondsBetweenTransfers_Pointer(t *testing.T) {
	t.Run("nil_becomes_60", func(t *testing.T) {
		cfg := Config{}
		out := withDefaults(cfg)
		if out.Rebalance.MinSecondsBetweenTransfers == nil {
			t.Fatal("withDefaults didn't allocate")
		}
		if got := *out.Rebalance.MinSecondsBetweenTransfers; got != 60 {
			t.Errorf("nil -> withDefaults: got %d; want 60", got)
		}
	})

	t.Run("explicit_zero_preserved", func(t *testing.T) {
		zero := uint32(0)
		cfg := Config{}
		cfg.Rebalance.MinSecondsBetweenTransfers = &zero
		out := withDefaults(cfg)
		if out.Rebalance.MinSecondsBetweenTransfers == nil {
			t.Fatal("withDefaults nil'd the explicit-0 pointer")
		}
		if got := *out.Rebalance.MinSecondsBetweenTransfers; got != 0 {
			t.Errorf("explicit 0 -> withDefaults: got %d; want preserved 0", got)
		}
	})

	t.Run("explicit_30_preserved", func(t *testing.T) {
		v := uint32(30)
		cfg := Config{}
		cfg.Rebalance.MinSecondsBetweenTransfers = &v
		out := withDefaults(cfg)
		if got := *out.Rebalance.MinSecondsBetweenTransfers; got != 30 {
			t.Errorf("explicit 30 -> withDefaults: got %d; want 30", got)
		}
	})
}
