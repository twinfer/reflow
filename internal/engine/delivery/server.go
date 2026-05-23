// Package delivery implements the inter-node Delivery Connect RPC service
// used for cross-partition outbox dispatch.
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
	"net/http"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine"
	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
	"github.com/twinfer/reflow/proto/deliveryv1/deliveryv1connect"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// RunnerView is re-exported from engine so unit tests can stub the
// runner without importing the heavy *PartitionRunner type.
type RunnerView = engine.RunnerView

// HostView is the slice of *engine.Host the Server depends on. Keeping
// the dependency narrow makes the server unit-testable without booting a
// full NodeHost. *engine.Host satisfies this (PartitionRunner satisfies
// RunnerView trivially).
type HostView interface {
	// PartitionRunner returns a RunnerView for shardID, or nil when
	// shardID is not hosted on this node.
	PartitionRunner(shardID uint64) RunnerView
	// MetadataRunnerView returns a RunnerView for shard 0 (the metadata
	// Raft group), or nil when shard 0 is not hosted on this node.
	// Used by the LP-mover ack path: partition shards send
	// UpdateLpTransferPhase via CrossShardSender to shard 0.
	MetadataRunnerView() RunnerView
	// PartitionLeaderHint returns the believed leader's NodeID for
	// shardID, sourced from dragonboat gossip. Used to populate
	// NotLeader hints on responses.
	PartitionLeaderHint(shardID uint64) (uint64, bool)
	// PartitionDataDir returns the per-shard on-disk dataDir (the Pebble
	// directory). The LP-transfer upload server writes staged SSTs into
	// `<dataDir>.lpstage_in/<transfer_id>/`. Returns ("", false) when
	// shardID is not hosted on this node.
	PartitionDataDir(shardID uint64) (string, bool)
}

// Server implements deliveryv1connect.DeliveryHandler over an *engine.Host.
type Server struct {
	deliveryv1connect.UnimplementedDeliveryHandler
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

// NewHandler returns the path + http.Handler pair to mount on a
// connectserver. opts is forwarded to the generated handler (e.g.
// connect.WithInterceptors for cross-cutting concerns).
func (s *Server) NewHandler(opts ...connect.HandlerOption) (string, http.Handler) {
	return deliveryv1connect.NewDeliveryHandler(s, opts...)
}

// Deliver is the bidi-stream handler. For each DeliverRequest received,
// the server attempts to propose the command via the destination shard's
// RaftProposer and replies with Ack / NotLeader / Err. The stream stays
// open until the sender closes its half or the context is cancelled.
func (s *Server) Deliver(ctx context.Context, stream *connect.BidiStream[deliveryv1.DeliverRequest, deliveryv1.DeliverResponse]) error {
	for {
		req, err := stream.Receive()
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
	if req.GetProducerId() == "" || req.GetCommand() == nil {
		return errResponse(req.GetSeq(), "delivery: malformed request")
	}

	var runner RunnerView
	var isLeader bool
	if shardID == 0 {
		// Shard-0 inbound deliveries are allowlisted: the only
		// expected cross-shard sender → shard-0 command today is
		// UpdateLpTransferPhase (LP-mover ack from a partition's
		// LPTransferService). Other shard-0 commands are
		// node-self-proposals; rejecting them defensively prevents a
		// compromised peer from re-running cluster ownership state
		// changes via the delivery surface.
		if !isAllowedShard0Inbound(req.GetCommand()) {
			s.log.Warn("delivery: rejected unsupported shard-0 inbound command",
				"producer", req.GetProducerId(), "kind", fmt.Sprintf("%T", req.GetCommand().GetKind()))
			return errResponse(req.GetSeq(), "delivery: command kind not allowed on shard 0")
		}
		runner = s.host.MetadataRunnerView()
	} else {
		runner = s.host.PartitionRunner(shardID)
	}
	if runner == nil {
		// Not hosted here. Fall through to the not-leader hint path; the
		// sender will re-resolve via gossip and dial the actual leader.
		return s.notLeaderResponse(req.GetSeq(), shardID)
	}
	isLeader = runner.IsLeader()
	if !isLeader {
		return s.notLeaderResponse(req.GetSeq(), shardID)
	}

	if err := runner.Proposer().ProposeIngress(ctx, req.GetProducerId(), req.GetSeq(), req.GetCommand()); err != nil {
		if errors.Is(err, engine.ErrShardClosed) {
			return s.notLeaderResponse(req.GetSeq(), shardID)
		}
		if connect.CodeOf(err) == connect.CodeUnavailable {
			return s.notLeaderResponse(req.GetSeq(), shardID)
		}
		s.log.Warn("delivery: ProposeIngress failed",
			"shard", shardID, "producer", req.GetProducerId(), "seq", req.GetSeq(), "err", err)
		return errResponse(req.GetSeq(), fmt.Sprintf("propose: %v", err))
	}
	return ackResponse(req.GetSeq())
}

// isAllowedShard0Inbound returns true when cmd is one of the few
// command kinds a partition's LPTransferService legitimately sends to
// shard 0. Anything else is rejected (defense in depth: cluster
// ownership commands like RegisterNode / UpdatePartitionTable /
// UpsertLPOwner must never arrive via delivery — they are
// self-proposed by the metadata leader only).
func isAllowedShard0Inbound(cmd *enginev1.Command) bool {
	switch cmd.GetKind().(type) {
	case *enginev1.Command_UpdateLpTransferPhase:
		return true
	}
	return false
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
