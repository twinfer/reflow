package engine

import (
	"testing"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

func TestBuildSelfProposalEnvelope(t *testing.T) {
	cmd := &enginev1.Command{Kind: &enginev1.Command_TimerFired{
		TimerFired: &enginev1.TimerFired{SleepIndex: 7},
	}}
	env := buildSelfProposalEnvelope(3, 42, cmd)
	d := env.GetHeader().GetDedup().GetSelfProposal()
	if d == nil {
		t.Fatalf("expected SelfProposal dedup, got %+v", env.GetHeader().GetDedup())
	}
	if d.GetLeaderEpoch() != 3 || d.GetSeq() != 42 {
		t.Errorf("epoch/seq mismatch: %+v", d)
	}
	if env.GetCommand().GetTimerFired().GetSleepIndex() != 7 {
		t.Errorf("command not preserved")
	}
}

func TestBuildIngressEnvelope(t *testing.T) {
	cmd := &enginev1.Command{Kind: &enginev1.Command_Purge{
		Purge: &enginev1.PurgeInvocation{},
	}}
	env := buildIngressEnvelope("producer-x", 100, cmd)
	d := env.GetHeader().GetDedup().GetArbitrary()
	if d == nil {
		t.Fatalf("expected Arbitrary dedup, got %+v", env.GetHeader().GetDedup())
	}
	if d.GetProducerId() != "producer-x" || d.GetSeq() != 100 {
		t.Errorf("producer/seq mismatch: %+v", d)
	}
}
