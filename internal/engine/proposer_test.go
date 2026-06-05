package engine

import (
	"context"
	"testing"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

func TestBuildSelfProposalEnvelope(t *testing.T) {
	cmd := &enginev1.Command{Kind: &enginev1.Command_TimerFired{
		TimerFired: &enginev1.TimerFired{SleepIndex: 7},
	}}
	env := buildSelfProposalEnvelope(context.Background(), 3, 42, cmd)
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
	if env.GetHeader().GetPrincipal() != "" {
		t.Errorf("self-proposal with empty ctx should have empty principal; got %q",
			env.GetHeader().GetPrincipal())
	}
}

func TestBuildSelfProposalEnvelope_StampsPrincipalFromCtx(t *testing.T) {
	cmd := &enginev1.Command{Kind: &enginev1.Command_TimerFired{
		TimerFired: &enginev1.TimerFired{SleepIndex: 1},
	}}
	ctx := WithProposalPrincipal(context.Background(), "operator/alice")
	env := buildSelfProposalEnvelope(ctx, 1, 1, cmd)
	if got := env.GetHeader().GetPrincipal(); got != "operator/alice" {
		t.Errorf("Header.principal = %q, want operator/alice", got)
	}
}

func TestWithProposalPrincipal_EmptyIsNoop(t *testing.T) {
	ctx := WithProposalPrincipal(context.Background(), "")
	env := buildSelfProposalEnvelope(ctx, 1, 1, &enginev1.Command{})
	if got := env.GetHeader().GetPrincipal(); got != "" {
		t.Errorf("empty WithProposalPrincipal should not stamp; got %q", got)
	}
}

func TestBuildIngressEnvelope(t *testing.T) {
	cmd := &enginev1.Command{Kind: &enginev1.Command_Purge{
		Purge: &enginev1.PurgeInvocation{},
	}}
	env := buildIngressEnvelope(context.Background(), "producer-x", 100, cmd)
	d := env.GetHeader().GetDedup().GetArbitrary()
	if d == nil {
		t.Fatalf("expected Arbitrary dedup, got %+v", env.GetHeader().GetDedup())
	}
	if d.GetProducerId() != "producer-x" || d.GetSeq() != 100 {
		t.Errorf("producer/seq mismatch: %+v", d)
	}
}
