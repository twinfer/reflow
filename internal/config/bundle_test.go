package config

import (
	"testing"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func TestValidateBundle(t *testing.T) {
	dmnRef := &enginev1.ModelRef{Kind: "dmn", Name: "Credit", Version: "v1"}
	bpmnRef := &enginev1.ModelRef{Kind: "bpmn", Name: "Sub", Version: "v1"}
	cases := []struct {
		name    string
		bundle  *enginev1.ModelBundle
		wantErr bool
	}{
		{"nil", nil, false},
		{"empty", &enginev1.ModelBundle{}, false},
		{"valid", &enginev1.ModelBundle{
			Decisions: map[string]*enginev1.ModelRef{"d": dmnRef},
			Children:  map[string]*enginev1.ModelRef{"c": bpmnRef},
		}, false},
		{"decision wrong kind", &enginev1.ModelBundle{
			Decisions: map[string]*enginev1.ModelRef{"d": bpmnRef}, // bpmn, not dmn
		}, true},
		{"decision no name", &enginev1.ModelBundle{
			Decisions: map[string]*enginev1.ModelRef{"d": {Kind: "dmn"}},
		}, true},
		{"child wrong kind", &enginev1.ModelBundle{
			Children: map[string]*enginev1.ModelRef{"c": dmnRef}, // dmn, not bpmn/cmmn
		}, true},
		{"child no name", &enginev1.ModelBundle{
			Children: map[string]*enginev1.ModelRef{"c": {Kind: "bpmn"}},
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBundle(tc.bundle)
			if tc.wantErr && err == nil {
				t.Fatal("validateBundle = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("validateBundle = %v, want nil", err)
			}
		})
	}
}
