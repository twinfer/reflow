package config_test

import (
	"testing"

	"github.com/twinfer/reflow/pkg/reflow/config"
)

// TestLoad_RebalanceMinSecondsBetweenTransfers_DecoderPointer asserts
// that koanf decodes a `min_seconds_between_transfers: 0` value into
// a non-nil pointer to 0 (operator intent: "no cooldown") and an
// absent key into a nil pointer (deferred default).
//
// The withDefaults-layer counterpart lives in
// pkg/reflow.TestWithDefaults_MinSecondsBetweenTransfers_Pointer.
func TestLoad_RebalanceMinSecondsBetweenTransfers_DecoderPointer(t *testing.T) {
	t.Run("absent_key_yields_nil", func(t *testing.T) {
		cfg, _, err := config.Load(config.FromMap(map[string]any{
			"rebalance": map[string]any{
				"mode": "auto",
			},
		}))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Rebalance.MinSecondsBetweenTransfers != nil {
			t.Errorf("absent key: got %v; want nil",
				*cfg.Rebalance.MinSecondsBetweenTransfers)
		}
	})

	t.Run("explicit_zero_yields_non_nil_zero", func(t *testing.T) {
		cfg, _, err := config.Load(config.FromMap(map[string]any{
			"rebalance": map[string]any{
				"mode":                          "auto",
				"min_seconds_between_transfers": 0,
			},
		}))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Rebalance.MinSecondsBetweenTransfers == nil {
			t.Fatal("explicit 0: got nil; want non-nil pointer to 0")
		}
		if got := *cfg.Rebalance.MinSecondsBetweenTransfers; got != 0 {
			t.Errorf("explicit 0: got %d; want 0", got)
		}
	})

	t.Run("explicit_value_yields_non_nil", func(t *testing.T) {
		cfg, _, err := config.Load(config.FromMap(map[string]any{
			"rebalance": map[string]any{
				"mode":                          "auto",
				"min_seconds_between_transfers": 30,
			},
		}))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got := cfg.Rebalance.MinSecondsBetweenTransfers; got == nil || *got != 30 {
			t.Errorf("explicit 30: got %v; want non-nil 30", got)
		}
	})
}
