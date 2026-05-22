package eventsource

import (
	"strings"
	"testing"
)

// TestValidate_UnknownType returns nil so the admin RPC's existing
// HasFactory check remains the single source of truth for "is this a
// real backend?" — there's no reason to duplicate that rule here.
func TestValidate_UnknownType(t *testing.T) {
	if err := Validate("does-not-exist", "anything", BackendConfig{}); err != nil {
		t.Errorf("Validate(unknown) = %v; want nil", err)
	}
}

// TestValidate_NilValidator covers the gochannel registration path:
// nil validator means "no extra rules," so any topic must pass.
func TestValidate_NilValidator(t *testing.T) {
	if err := Validate("gochannel", "weird.topic.with.dots", BackendConfig{}); err != nil {
		t.Errorf("Validate(gochannel) = %v; want nil", err)
	}
}

func TestValidate_NATS(t *testing.T) {
	cases := []struct {
		name    string
		topic   string
		backend BackendConfig
		wantErr string // substring; empty = expect nil
	}{
		{name: "valid topic", topic: "orders_created"},
		{name: "valid with dash", topic: "orders-v2"},
		{name: "valid with digits", topic: "topic42"},
		{name: "dotted topic rejected", topic: "orders.created", wantErr: "illegal char"},
		{name: "wildcard rejected", topic: "orders.*", wantErr: "illegal char"},
		{name: "tail rejected", topic: "orders.>", wantErr: "illegal char"},
		{name: "space rejected", topic: "bad name", wantErr: "illegal char"},
		{name: "valid durable", topic: "ok", backend: BackendConfig{Settings: map[string]string{"durable_prefix": "reflow_dur"}}},
		{name: "dotted durable rejected", topic: "ok", backend: BackendConfig{Settings: map[string]string{"durable_prefix": "reflow.dur"}}, wantErr: "durable_prefix"},
		{name: "empty durable accepted", topic: "ok", backend: BackendConfig{Settings: map[string]string{"durable_prefix": ""}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate("nats", tc.topic, tc.backend)
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("Validate = %v; want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Validate = %v; want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidate_Kafka(t *testing.T) {
	cases := []struct {
		name    string
		topic   string
		wantErr string
	}{
		{name: "dotted ok", topic: "orders.created.v1"},
		{name: "underscored ok", topic: "orders_created"},
		{name: "mixed ok", topic: "orders.v2_2026"},
		{name: "dashed ok", topic: "orders-v2"},
		{name: "too long", topic: strings.Repeat("a", 250), wantErr: "max 249"},
		{name: "exactly 249 chars ok", topic: strings.Repeat("a", 249)},
		{name: "slash rejected", topic: "orders/v2", wantErr: "illegal char"},
		{name: "space rejected", topic: "bad name", wantErr: "illegal char"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate("kafka", tc.topic, BackendConfig{})
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("Validate = %v; want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Validate = %v; want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidate_SQS(t *testing.T) {
	cases := []struct {
		name    string
		topic   string
		wantErr string
	}{
		{name: "alphanumeric ok", topic: "reflow-orders"},
		{name: "with underscore ok", topic: "reflow_orders"},
		{name: "fifo suffix ok", topic: "reflow-orders.fifo"},
		{name: "exactly 80 chars ok", topic: strings.Repeat("a", 80)},
		{name: "81 chars too long", topic: strings.Repeat("a", 81), wantErr: "max 80"},
		{name: "dotted (non-fifo) rejected", topic: "orders.created", wantErr: "illegal char"},
		{name: "slash rejected", topic: "orders/v1", wantErr: "illegal char"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate("sqs", tc.topic, BackendConfig{})
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("Validate = %v; want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Validate = %v; want substring %q", err, tc.wantErr)
			}
		})
	}
}
