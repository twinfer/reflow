package limits

import (
	"testing"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func TestEffectiveMaxJournalEntries(t *testing.T) {
	cases := []struct {
		name string
		rec  *enginev1.DeploymentRecord
		want uint32
	}{
		{"nil → default", nil, DefaultMaxJournalEntries},
		{"zero → default", &enginev1.DeploymentRecord{}, DefaultMaxJournalEntries},
		{"override under ceiling", &enginev1.DeploymentRecord{MaxJournalEntries: 500}, 500},
		{"override at ceiling", &enginev1.DeploymentRecord{MaxJournalEntries: MaxAllowedJournalEntries}, MaxAllowedJournalEntries},
		{"override above ceiling clamps", &enginev1.DeploymentRecord{MaxJournalEntries: 1 << 30}, MaxAllowedJournalEntries},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := EffectiveMaxJournalEntries(c.rec); got != c.want {
				t.Errorf("got %d; want %d", got, c.want)
			}
		})
	}
}

func TestEffectiveRetentionMs(t *testing.T) {
	cases := []struct {
		name    string
		rec     *enginev1.DeploymentRecord
		wantInv uint64
		wantWf  uint64
	}{
		{"nil → defaults", nil, DefaultInvocationRetentionMs, DefaultWorkflowRetentionMs},
		{"zero → defaults", &enginev1.DeploymentRecord{}, DefaultInvocationRetentionMs, DefaultWorkflowRetentionMs},
		{
			"override under ceiling",
			&enginev1.DeploymentRecord{InvocationRetentionMs: 1000, WorkflowRetentionMs: 2000},
			1000, 2000,
		},
		{
			"override above ceiling clamps",
			&enginev1.DeploymentRecord{InvocationRetentionMs: MaxAllowedRetentionMs + 1, WorkflowRetentionMs: 1 << 62},
			MaxAllowedRetentionMs, MaxAllowedRetentionMs,
		},
		{
			"one overridden, other defaults",
			&enginev1.DeploymentRecord{InvocationRetentionMs: 5000},
			5000, DefaultWorkflowRetentionMs,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := EffectiveInvocationRetentionMs(c.rec); got != c.wantInv {
				t.Errorf("invocation: got %d; want %d", got, c.wantInv)
			}
			if got := EffectiveWorkflowRetentionMs(c.rec); got != c.wantWf {
				t.Errorf("workflow: got %d; want %d", got, c.wantWf)
			}
		})
	}
}
