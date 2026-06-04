package ingress

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"slices"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/storage/keys"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

const (
	// defaultListLimit caps a list response (ListInvocations /
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

// pageCursor is a decoded ListInvocations / ListProcessInstances continuation:
// the owning shard plus the full storage key of the last row returned on the
// prior page. Global iteration order is (shard asc, raw-key asc), so a shard <
// cursor.shard was fully drained in a prior page, cursor.shard resumes strictly
// past cursor.key (SeekGE), and shards > cursor.shard are scanned fresh. Stable
// only under stable LP ownership (the same caveat capped lists already carry).
type pageCursor struct {
	shard uint64
	key   []byte
}

// encodePageToken renders a cursor as an opaque base64 token (8-byte big-endian
// shard || raw key). The embedded key is data the caller already saw in the
// page; a forged token only shifts the resume point within the same namespace
// scan, so it can't widen access.
func encodePageToken(shard uint64, key []byte) string {
	buf := make([]byte, 8+len(key))
	binary.BigEndian.PutUint64(buf[:8], shard)
	copy(buf[8:], key)
	return base64.RawURLEncoding.EncodeToString(buf)
}

// decodePageToken parses a token from a prior next_page_token. Empty → nil (start
// from the beginning).
func decodePageToken(tok string) (*pageCursor, error) {
	if tok == "" {
		return nil, nil
	}
	buf, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return nil, fmt.Errorf("malformed page token: %w", err)
	}
	if len(buf) < 8 {
		return nil, fmt.Errorf("malformed page token: short")
	}
	return &pageCursor{shard: binary.BigEndian.Uint64(buf[:8]), key: buf[8:]}, nil
}

// fanOut issues one linearizable SyncRead per partition shard, each scanning the
// whole namespace on that shard. Every node replicates every shard, so the read
// is always local — no cross-node RPC. The shard set is the distinct owners in
// the LPOwners snapshot (the full LP enumeration is the pre-warmup fallback).
// makeQuery builds that shard's engine Lookup from an optional resume key (the
// page cursor, non-nil only for the shard the cursor names); collect receives
// each shard's id + result and returns done=true once the caller has filled its
// row cap. Shards are visited in id order so a capped/paged result is stable
// given stable ownership. cur, when non-nil, resumes a prior page: shards below
// cur.shard are skipped (already drained) and cur.shard is seeked past cur.key.
// Shared substrate behind ListInvocations and ListProcessInstances.
func (s *Server) fanOut(ctx context.Context, cur *pageCursor, makeQuery func(shard uint64, after []byte) any, collect func(shard uint64, res any) (done bool, err error)) error {
	part := s.host.Partitioner()
	shardSet := make(map[uint64]struct{})
	if snap := part.LPOwnersSnapshot(); len(snap) > 0 {
		for _, shard := range snap {
			shardSet[shard] = struct{}{}
		}
	} else {
		for lp := range keys.LPCount {
			shardSet[part.ShardForLP(lp)] = struct{}{}
		}
	}
	shards := make([]uint64, 0, len(shardSet))
	for shard := range shardSet {
		shards = append(shards, shard)
	}
	slices.Sort(shards)
	for _, shard := range shards {
		if cur != nil && shard < cur.shard {
			continue // fully drained in a prior page
		}
		var after []byte
		if cur != nil && shard == cur.shard {
			after = cur.key
		}
		res, err := s.host.NodeHost().SyncRead(ctx, shard, makeQuery(shard, after))
		if err != nil {
			return fmt.Errorf("shard %d: %w", shard, err)
		}
		done, err := collect(shard, res)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	return nil
}

// ListInvocations lists the deployment's invocations: one whole-namespace scan
// per partition shard via the shared fanOut substrate, merging up to limit rows.
func (s *Server) ListInvocations(ctx context.Context, req *connect.Request[ingressv1.ListInvocationsRequest]) (*connect.Response[ingressv1.ListInvocationsResponse], error) {
	msg := req.Msg
	cur, err := decodePageToken(msg.GetPageToken())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	limit := clampListLimit(int(msg.GetLimit()))
	var out []*ingressv1.InvocationSummary
	var nextToken string
	ferr := s.fanOut(ctx, cur,
		func(shard uint64, after []byte) any {
			return engine.LookupInvocations{
				Service:         msg.GetService(),
				StateFilter:     msg.GetStateFilter(),
				CreatedAfterMs:  msg.GetCreatedAfterMs(),
				CreatedBeforeMs: msg.GetCreatedBeforeMs(),
				After:           after,
				Limit:           limit,
			}
		},
		func(shard uint64, res any) (bool, error) {
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
					key, err := keys.InvocationKey(iv.ID)
					if err != nil {
						return false, err
					}
					nextToken = encodePageToken(shard, key)
					return true, nil
				}
			}
			return false, nil
		},
	)
	if ferr != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("list invocations: %w", ferr))
	}
	return connect.NewResponse(&ingressv1.ListInvocationsResponse{Invocations: out, NextPageToken: nextToken}), nil
}
