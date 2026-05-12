// Package delivery implements the inter-node Delivery gRPC service used
// for cross-partition outbox dispatch in Phase 4.1.
//
// Sender side (Client): when a shard's leader-side OutboxService sees an
// OutboxEnvelope addressed to a different shard, it dials the destination
// shard's current leader (resolved via dragonboat gossip) and streams the
// envelope as a DeliverRequest. Receiver side (Server): the local Delivery
// implementation looks up the destination shard's RaftProposer and calls
// ProposeIngress; the inbound command flows through Raft and lands in the
// destination FSM exactly like a local-ingress command. The producer-id +
// seq carried in the request matches the sender's outbox row so the
// receiver's dedup layer absorbs replayed sends on crash recovery.
package delivery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twinfer/reflow/internal/engine"
	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
)

// RunnerView is the slice of *engine.PartitionRunner the Server uses. A
// shrink interface so unit tests can stub the runner.
type RunnerView interface {
	IsLeader() bool
	Proposer() *engine.RaftProposer
}

// HostView is the slice of *engine.Host the Server depends on. Keeping
// the dependency narrow makes the server unit-testable without booting a
// full NodeHost. *engine.Host satisfies this (PartitionRunner satisfies
// RunnerView trivially).
type HostView interface {
	// PartitionRunner returns a RunnerView for shardID, or nil when
	// shardID is not hosted on this node.
	PartitionRunner(shardID uint64) RunnerView
	// PartitionLeaderHint returns the believed leader's NodeID for
	// shardID, sourced from dragonboat gossip. Used to populate
	// NotLeader hints on responses.
	PartitionLeaderHint(shardID uint64) (uint64, bool)
}

// Server implements deliveryv1.DeliveryServer over an *engine.Host.
type Server struct {
	deliveryv1.UnimplementedDeliveryServer
	host HostView
	log  *slog.Logger
}

// NewServer wires a Server against a HostView (*engine.Host satisfies it).
func NewServer(host HostView, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{host: host, log: log}
}

// Deliver is the bidi-stream handler. For each DeliverRequest received,
// the server attempts to propose the command via the destination shard's
// RaftProposer and replies with Ack / NotLeader / Err. The stream stays
// open until the sender closes its half or the context is cancelled.
func (s *Server) Deliver(stream deliveryv1.Delivery_DeliverServer) error {
	ctx := stream.Context()
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		resp := s.handle(ctx, req)
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// handle processes a single DeliverRequest. Splits out from Deliver so
// unit tests can drive the per-message logic without a real stream.
func (s *Server) handle(ctx context.Context, req *deliveryv1.DeliverRequest) *deliveryv1.DeliverResponse {
	shardID := req.GetShardId()
	if shardID == 0 || req.GetProducerId() == "" || req.GetCommand() == nil {
		return errResponse(req.GetSeq(), "delivery: malformed request")
	}

	runner := s.host.PartitionRunner(shardID)
	if runner == nil {
		// Not hosted here. Fall through to the not-leader hint path; the
		// sender will re-resolve via gossip and dial the actual leader.
		return s.notLeaderResponse(req.GetSeq(), shardID)
	}
	if !runner.IsLeader() {
		return s.notLeaderResponse(req.GetSeq(), shardID)
	}

	if err := runner.Proposer().ProposeIngress(ctx, req.GetProducerId(), req.GetSeq(), req.GetCommand()); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return s.notLeaderResponse(req.GetSeq(), shardID)
		}
		if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
			return s.notLeaderResponse(req.GetSeq(), shardID)
		}
		s.log.Warn("delivery: ProposeIngress failed",
			"shard", shardID, "producer", req.GetProducerId(), "seq", req.GetSeq(), "err", err)
		return errResponse(req.GetSeq(), fmt.Sprintf("propose: %v", err))
	}
	return ackResponse(req.GetSeq())
}

// notLeaderResponse builds a NotLeader reply, populating leader_node_id
// from gossip when available.
func (s *Server) notLeaderResponse(seq, shardID uint64) *deliveryv1.DeliverResponse {
	leaderID, _ := s.host.PartitionLeaderHint(shardID)
	return &deliveryv1.DeliverResponse{
		Seq: seq,
		Kind: &deliveryv1.DeliverResponse_NotLeader{
			NotLeader: &deliveryv1.NotLeader{LeaderNodeId: leaderID},
		},
	}
}

func ackResponse(seq uint64) *deliveryv1.DeliverResponse {
	return &deliveryv1.DeliverResponse{
		Seq:  seq,
		Kind: &deliveryv1.DeliverResponse_Ack{Ack: &deliveryv1.Ack{}},
	}
}

func errResponse(seq uint64, msg string) *deliveryv1.DeliverResponse {
	return &deliveryv1.DeliverResponse{
		Seq:  seq,
		Kind: &deliveryv1.DeliverResponse_Err{Err: &deliveryv1.Err{Message: msg}},
	}
}
