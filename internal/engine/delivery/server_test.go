package delivery

import (
	"context"
	"testing"

	"github.com/twinfer/reflw/internal/engine"
	deliveryv1 "github.com/twinfer/reflw/proto/deliveryv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

type fakeRunner struct {
	leader bool
	prop   *engine.RaftProposer
}

func (r *fakeRunner) IsLeader() bool                 { return r.leader }
func (r *fakeRunner) Proposer() *engine.RaftProposer { return r.prop }

type fakeHost struct {
	runners  map[uint64]RunnerView
	leader   map[uint64]uint64
	metadata RunnerView
	dataDirs map[uint64]string
}

func (h *fakeHost) PartitionRunner(shardID uint64) RunnerView {
	r, ok := h.runners[shardID]
	if !ok {
		return nil
	}
	return r
}
func (h *fakeHost) MetadataRunnerView() RunnerView {
	return h.metadata
}
func (h *fakeHost) PartitionLeaderHint(shardID uint64) (uint64, bool) {
	if id, ok := h.leader[shardID]; ok {
		return id, true
	}
	return 0, false
}
func (h *fakeHost) PartitionDataDir(shardID uint64) (string, bool) {
	d, ok := h.dataDirs[shardID]
	return d, ok
}

func TestDeliveryServer_NotHosted_ReturnsNotLeaderHint(t *testing.T) {
	s := NewServer(&fakeHost{
		runners: map[uint64]RunnerView{},
		leader:  map[uint64]uint64{7: 42},
	}, nil)
	resp := s.handle(context.Background(), &deliveryv1.DeliverRequest{
		ShardId:    7,
		ProducerId: "outbox/p2",
		Seq:        1,
		Command:    &enginev1.Command{},
	})
	nl, ok := resp.GetKind().(*deliveryv1.DeliverResponse_NotLeader)
	if !ok {
		t.Fatalf("expected NotLeader; got %T", resp.GetKind())
	}
	if nl.NotLeader.GetLeaderNodeId() != 42 {
		t.Errorf("leader hint = %d; want 42", nl.NotLeader.GetLeaderNodeId())
	}
	if resp.GetSeq() != 1 {
		t.Errorf("seq = %d; want 1", resp.GetSeq())
	}
}

func TestDeliveryServer_NotLeader_ReturnsNotLeader(t *testing.T) {
	s := NewServer(&fakeHost{
		runners: map[uint64]RunnerView{
			7: &fakeRunner{leader: false},
		},
	}, nil)
	resp := s.handle(context.Background(), &deliveryv1.DeliverRequest{
		ShardId:    7,
		ProducerId: "outbox/p2",
		Seq:        5,
		Command:    &enginev1.Command{},
	})
	if _, ok := resp.GetKind().(*deliveryv1.DeliverResponse_NotLeader); !ok {
		t.Fatalf("expected NotLeader; got %T", resp.GetKind())
	}
}

func TestDeliveryServer_MalformedReturnsErr(t *testing.T) {
	s := NewServer(&fakeHost{}, nil)
	resp := s.handle(context.Background(), &deliveryv1.DeliverRequest{
		// missing ShardId, ProducerId, Command
		Seq: 9,
	})
	errKind, ok := resp.GetKind().(*deliveryv1.DeliverResponse_Err)
	if !ok {
		t.Fatalf("expected Err; got %T", resp.GetKind())
	}
	if errKind.Err.GetMessage() == "" {
		t.Error("expected non-empty Err.message")
	}
	if resp.GetSeq() != 9 {
		t.Errorf("seq = %d; want 9", resp.GetSeq())
	}
}
