package adminclient

import (
	"context"
	"errors"
	"fmt"

	connect "connectrpc.com/connect"

	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	"github.com/twinfer/reflow/proto/adminv1/adminv1connect"
)

// CallWithLeaderRedirect invokes fn against opts.Addr; on
// connect.CodeUnavailable carrying a LeaderHint detail, it re-dials the
// hinted admin endpoint (reusing opts.Creds) and retries. Bounded at
// maxHops to break loops in degraded clusters where the hint cycles
// between non-leaders. Returns the last RPC error when hops are
// exhausted or when the error is non-Unavailable / lacks a hint.
//
// Used by:
//   - the joiner's callSelfJoin path in pkg/reflow/run.go (initial dial
//     comes from gossip-resolved leader admin endpoint; redirect is the
//     safety net for one-heartbeat-stale gossip);
//   - the reflow-cluster CLI, whose --admin flag means "any cluster
//     node" — every mutating command wraps its RPC in this helper.
func CallWithLeaderRedirect(
	ctx context.Context,
	opts DialOptions,
	maxHops int,
	fn func(context.Context, adminv1connect.AdminClient) error,
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
		err = fn(ctx, cli.Admin)
		_ = cli.Close()
		if err == nil {
			return nil
		}
		lastErr = err
		var connectErr *connect.Error
		if !errors.As(err, &connectErr) || connectErr.Code() != connect.CodeUnavailable {
			return err
		}
		hint := extractLeaderHint(connectErr)
		if hint == nil || hint.GetAdminEndpoint() == "" {
			return err
		}
		if hint.GetAdminEndpoint() == opts.Addr {
			// Server hints at itself or at the address we just failed
			// against — further redirects would loop.
			return err
		}
		opts.Addr = hint.GetAdminEndpoint()
	}
	return fmt.Errorf("admin: leader redirect exhausted after %d hops: %w", maxHops, lastErr)
}

// extractLeaderHint walks the Connect error's protobuf details for the
// first *adminv1.LeaderHint. Connect serialises details as
// google.protobuf.Any so we rely on the generated message type for
// decoding.
func extractLeaderHint(cerr *connect.Error) *adminv1.LeaderHint {
	for _, d := range cerr.Details() {
		v, err := d.Value()
		if err != nil {
			continue
		}
		if h, ok := v.(*adminv1.LeaderHint); ok {
			return h
		}
	}
	return nil
}
