package clusterctl

import (
	"context"
	"errors"
	"fmt"

	connect "connectrpc.com/connect"
	"github.com/google/uuid"

	"github.com/twinfer/reflow/internal/storage/keys"
	clusterctlv1 "github.com/twinfer/reflow/proto/clusterctlv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TransferLP initiates a cross-shard transfer for one logical
// partition. Validates inputs (lp range, dest_shard is a real shard,
// not already owner) against the current PartitionTable + LPOwners,
// mints a UUIDv4 transfer_id, and proposes
// Command_InitiateLPTransfer to shard 0. The lpMover goroutine on the
// metadata leader picks the new row up on the next tick (or
// immediately, via the LPTransfersTable notifier) and drives the
// saga. Returns once the proposal commits.
//
// Leader-only. Concurrent admin retries against a stale revision
// don't apply here (the InitiateLPTransfer arm has its own internal
// dedup against the LP's in-progress state).
func (s *Server) TransferLP(ctx context.Context, req *connect.Request[clusterctlv1.TransferLPRequest]) (*connect.Response[clusterctlv1.TransferLPResponse], error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	lp := req.Msg.GetLp()
	destShard := req.Msg.GetDestShard()
	if lp >= keys.LPCount {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("clusterctl: lp %d out of range [0, %d)", lp, keys.LPCount))
	}
	if destShard == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("clusterctl: dest_shard must be a partition shard id (>= 1)"))
	}
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	// Validate dest_shard against the current partition table and lp's
	// current owner via SyncRead. Belt-and-suspenders — the
	// applyInitiateLPTransfer arm validates again on the apply side, so
	// even on validation drift the FSM stays consistent.
	pt, err := s.host.PartitionTable(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("clusterctl: read partition table: %w", err))
	}
	if pt == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("clusterctl: partition table not yet bootstrapped"))
	}
	if _, ok := pt.GetShards()[destShard]; !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("clusterctl: dest_shard %d is not a known partition shard", destShard))
	}
	owners, err := s.host.LPOwners(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("clusterctl: read lp owners: %w", err))
	}
	var currentOwner uint64
	for _, rec := range owners.Records {
		if rec.GetLp() == lp {
			currentOwner = rec.GetShardId()
			break
		}
	}
	if currentOwner == 0 {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("clusterctl: lp %d has no current owner (table not yet seeded)", lp))
	}
	if currentOwner == destShard {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("clusterctl: lp %d already on shard %d", lp, destShard))
	}
	transferID := uuid.NewString()
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_InitiateLpTransfer{
			InitiateLpTransfer: &enginev1.InitiateLPTransfer{
				TransferId: transferID,
				Lp:         lp,
				DestShard:  destShard,
			},
		},
	}
	if err := s.runner.Proposer().ProposeSelf(callCtx, cmd); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("clusterctl: propose InitiateLPTransfer: %w", err))
	}
	return connect.NewResponse(&clusterctlv1.TransferLPResponse{
		TransferId: transferID,
	}), nil
}

// ListLPTransfers returns every LPTransferRecord plus the table's
// current CAS revision. No leader gate — SyncRead routes to the local
// shard-0 replica so any peer can serve. Operators use this to poll
// transfer progress after issuing TransferLP.
func (s *Server) ListLPTransfers(ctx context.Context, _ *connect.Request[clusterctlv1.ListLPTransfersRequest]) (*connect.Response[clusterctlv1.ListLPTransfersResponse], error) {
	callCtx, cancel := context.WithTimeout(ctx, s.adminCallTimeout)
	defer cancel()
	list, err := s.host.LPTransfers(callCtx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("clusterctl: read lp transfers: %w", err))
	}
	return connect.NewResponse(&clusterctlv1.ListLPTransfersResponse{
		Records:       list.Records,
		TableRevision: list.TableRevision,
	}), nil
}
