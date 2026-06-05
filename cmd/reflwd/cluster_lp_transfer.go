package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/pkg/reflwclient"
	clusterctlv1 "github.com/twinfer/reflw/proto/clusterctlv1"
)

// cmdTransferLP invokes ClusterCtl/TransferLP to initiate a cross-shard
// LP transfer. Follows the leader-redirect pattern — the call lands on
// the metadata leader even if --admin points at a follower.
//
//	reflwd cluster transfer-lp --lp=N --to-shard=M [--admin=ADDR]
func cmdTransferLP(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("transfer-lp", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	lp := fs.Uint("lp", 0, "logical partition id (0..4095, required)")
	destShard := fs.Uint64("to-shard", 0, "destination partition shard id (>=1, required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *destShard == 0 {
		return errors.New("--to-shard is required (must be a partition shard id, not 0)")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflwclient.Client) error {
		resp, err := cli.Cluster.TransferLP(rctx, connect.NewRequest(&clusterctlv1.TransferLPRequest{
			Lp:        uint32(*lp),
			DestShard: *destShard,
		}))
		if err != nil {
			return err
		}
		fmt.Printf("transfer initiated: transfer_id=%s\n", resp.Msg.GetTransferId())
		return nil
	})
}

// cmdListLPTransfers invokes ClusterCtl/ListLPTransfers and emits the
// returned rows as indented JSON. Read-only — any peer can answer
// (SyncRead against shard 0).
//
//	reflwd cluster list-lp-transfers [--admin=ADDR]
func cmdListLPTransfers(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list-lp-transfers", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *reflwclient.Client) error {
		resp, err := cli.Cluster.ListLPTransfers(ctx, connect.NewRequest(&clusterctlv1.ListLPTransfersRequest{}))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"table_revision": resp.Msg.GetTableRevision(),
			"records":        resp.Msg.GetRecords(),
		})
	})
}
