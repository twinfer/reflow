package reflwclient

import (
	"context"
	"errors"
	"fmt"

	connect "connectrpc.com/connect"

	clusterctlv1 "github.com/twinfer/reflw/proto/clusterctlv1"
	configv1 "github.com/twinfer/reflw/proto/configv1"
)

// CallWithLeaderRedirect invokes fn against opts.Addr; on
// connect.CodeUnavailable carrying a LeaderHint detail (either
// clusterctlv1.LeaderHint or configv1.LeaderHint), it re-dials the
// hinted admin endpoint (reusing opts.Creds) and retries. Bounded at
// maxHops to break loops in degraded clusters where the hint cycles
// between non-leaders. Returns the last RPC error when hops are
// exhausted or when the error is non-Unavailable / lacks a hint.
//
// fn receives the full *Client wrapper so callers can pick the right
// typed sub-client (cli.Cluster.AddNode, cli.Config.UpsertSecret, …).
//
// Used by:
//   - the joiner's callSelfJoin path in pkg/reflw/run.go (initial dial
//     comes from gossip-resolved leader admin endpoint; redirect is the
//     safety net for one-heartbeat-stale gossip);
//   - the `reflwd cluster ...` / `reflwd config ...` CLIs, whose
//     --admin flag means "any cluster node" — every mutating command
//     wraps its RPC in this helper.
func CallWithLeaderRedirect(
	ctx context.Context,
	opts DialOptions,
	maxHops int,
	fn func(context.Context, *Client) error,
) error {
	if maxHops < 1 {
		maxHops = 1
	}
	var lastErr error
	for hop := 0; hop < maxHops; hop++ {
		cli, err := Dial(ctx, opts)
		if err != nil {
			return err
		}
		err = fn(ctx, cli)
		_ = cli.Close()
		if err == nil {
			return nil
		}
		lastErr = err
		var connectErr *connect.Error
		if !errors.As(err, &connectErr) || connectErr.Code() != connect.CodeUnavailable {
			return err
		}
		hintAddr := extractLeaderHint(connectErr)
		if hintAddr == "" {
			return err
		}
		if hintAddr == opts.Addr {
			// Server hints at itself or at the address we just failed
			// against — further redirects would loop.
			return err
		}
		opts.Addr = hintAddr
	}
	return fmt.Errorf("reflwclient: leader redirect exhausted after %d hops: %w", maxHops, lastErr)
}

// extractLeaderHint walks the Connect error's protobuf details for the
// first LeaderHint (either ClusterCtl's or Config's flavor — same
// shape, different proto-package URL). Returns the admin_endpoint, or
// "" when no hint is present.
func extractLeaderHint(cerr *connect.Error) string {
	for _, d := range cerr.Details() {
		v, err := d.Value()
		if err != nil {
			continue
		}
		if h, ok := v.(*clusterctlv1.LeaderHint); ok {
			return h.GetAdminEndpoint()
		}
		if h, ok := v.(*configv1.LeaderHint); ok {
			return h.GetAdminEndpoint()
		}
	}
	return ""
}
