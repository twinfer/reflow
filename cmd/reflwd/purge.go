package main

import (
	"context"
	"errors"
	"flag"
	"fmt"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/pkg/reflwclient"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// cmdPurgeInvocation immediately deletes a Completed invocation's durable
// rows (status, journal, signal inbox/awaiter) via the ingress
// PurgeInvocation RPC, rather than waiting for the retention reaper. It is
// operator-only (the procedure is gated out of the anonymous IngressActions
// plane), so it dials the ingress listener with the operator cert.
//
// The RPC routes to the partition owning the invocation id, so --ingress
// must name a node hosting that shard; against a node that doesn't, the
// server returns FailedPrecondition and the operator retries another node.
// No leader redirect — the propose mirrors SubmitInvocation's data-plane path.
func cmdPurgeInvocation(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("purge-invocation", flag.ContinueOnError)
	tls := registerIngressTLSFlags(fs)
	id := fs.String("id", "", "invocation id to purge")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	return tls.withClient(ctx, func(cli *reflwclient.Client) error {
		resp, err := cli.Ingress.PurgeInvocation(ctx, connect.NewRequest(&ingressv1.PurgeInvocationRequest{
			InvocationId: *id,
		}))
		if err != nil {
			return err
		}
		fmt.Printf("PurgeInvocation accepted=%v (id=%s)\n", resp.Msg.GetAccepted(), *id)
		return nil
	})
}
