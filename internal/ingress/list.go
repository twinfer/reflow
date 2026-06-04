package ingress

import (
	"context"
	"fmt"
	"slices"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/storage/keys"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

const (
	// defaultListLimit caps a band-list response (ListInvocations /
	// ListProcessInstances) when the caller gives no (or an over-large) limit;
	// maxListLimit is the hard ceiling.
	defaultListLimit = 1000
	maxListLimit     = 10000
)

// clampListLimit normalises a caller-supplied list limit to (0, maxListLimit].
func clampListLimit(limit int) int {
	if limit <= 0 || limit > maxListLimit {
		return defaultListLimit
	}
	return limit
}

// fanOutBand enumerates tenant band's LPs, groups them by owning shard, and
// issues one linearizable SyncRead per shard. Every node replicates every shard,
// so the read is always local — no cross-node RPC. makeQuery builds that shard's
// engine Lookup from its LP subset; collect receives each shard's result and
// returns done=true once the caller has filled its row cap. Shards are visited in
// id order so a capped result is stable given stable ownership. Shared substrate
// behind ListInvocations and ListProcessInstances.
func (s *Server) fanOutBand(ctx context.Context, band uint32, makeQuery func(lps []uint32) any, collect func(res any) (done bool, err error)) error {
	part := s.host.Partitioner()
	byShard := make(map[uint64][]uint32)
	lo := band << keys.IntraLPBits
	hi := (band + 1) << keys.IntraLPBits
	for lp := lo; lp < hi; lp++ {
		shard := part.ShardForLP(lp)
		byShard[shard] = append(byShard[shard], lp)
	}
	shards := make([]uint64, 0, len(byShard))
	for shard := range byShard {
		shards = append(shards, shard)
	}
	slices.Sort(shards)
	for _, shard := range shards {
		res, err := s.host.NodeHost().SyncRead(ctx, shard, makeQuery(byShard[shard]))
		if err != nil {
			return fmt.Errorf("shard %d: %w", shard, err)
		}
		done, err := collect(res)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	return nil
}

// ListInvocations lists the caller tenant band's invocations. Like
// ListProcessInstances it enumerates only the band's LPs, groups them by owning
// shard, and reads each shard locally via the shared fanOutBand substrate,
// merging up to limit rows. Tenant isolation: only the caller's band LPs are
// scanned, and each shard re-derives the band as defense in depth.
func (s *Server) ListInvocations(ctx context.Context, req *connect.Request[ingressv1.ListInvocationsRequest]) (*connect.Response[ingressv1.ListInvocationsResponse], error) {
	msg := req.Msg
	tenant, terr := principalTenant(ctx)
	if terr != nil {
		return nil, terr
	}
	limit := clampListLimit(int(msg.GetLimit()))
	var out []*ingressv1.InvocationSummary
	err := s.fanOutBand(ctx, tenant,
		func(lps []uint32) any {
			return engine.LookupInvocations{
				Tenant:      tenant,
				Service:     msg.GetService(),
				StateFilter: msg.GetStateFilter(),
				LPs:         lps,
				Limit:       limit,
			}
		},
		func(res any) (bool, error) {
			r, ok := res.(engine.InvocationsLookupResult)
			if !ok {
				return false, fmt.Errorf("unexpected result type %T", res)
			}
			for _, iv := range r.Invocations {
				out = append(out, &ingressv1.InvocationSummary{
					Id:            iv.ID,
					Target:        iv.Target,
					State:         iv.State,
					DeploymentId:  iv.DeploymentID,
					CreatedAtMs:   iv.CreatedAtMs,
					CompletedAtMs: iv.CompletedAtMs,
				})
				if len(out) >= limit {
					return true, nil
				}
			}
			return false, nil
		},
	)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("list invocations: %w", err))
	}
	return connect.NewResponse(&ingressv1.ListInvocationsResponse{Invocations: out}), nil
}
