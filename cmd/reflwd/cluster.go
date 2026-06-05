package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/pkg/reflowclient"
	clusterctlv1 "github.com/twinfer/reflw/proto/clusterctlv1"
)

func cmdAddNode(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("add-node", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	nodeID := fs.Uint64("node-id", 0, "node ID (required)")
	raftAddr := fs.String("raft-addr", "", "Raft listen addr (required)")
	gossipAddr := fs.String("gossip-addr", "", "gossip listen addr (required)")
	grpcEndpoint := fs.String("grpc-endpoint", "", "Delivery gRPC endpoint (required)")
	nhID := fs.String("node-host-id", "", "NodeHostID override (derived from node-id when empty)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *nodeID == 0 || *raftAddr == "" || *gossipAddr == "" || *grpcEndpoint == "" {
		return errors.New("--node-id, --raft-addr, --gossip-addr, --grpc-endpoint are required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Cluster.AddNode(rctx, connect.NewRequest(&clusterctlv1.AddNodeRequest{
			NodeId:       *nodeID,
			RaftAddr:     *raftAddr,
			GossipAddr:   *gossipAddr,
			GrpcEndpoint: *grpcEndpoint,
			NodeHostId:   *nhID,
		}))
		if err != nil {
			return err
		}
		fmt.Printf("AddNode ok (assignment_epoch=%d)\n", resp.Msg.GetAssignmentEpoch())
		return nil
	})
}

func cmdRemoveNode(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("remove-node", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	nodeID := fs.Uint64("node-id", 0, "node ID (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *nodeID == 0 {
		return errors.New("--node-id is required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Cluster.RemoveNode(rctx, connect.NewRequest(&clusterctlv1.RemoveNodeRequest{NodeId: *nodeID}))
		if err != nil {
			return err
		}
		fmt.Printf("RemoveNode ok (assignment_epoch=%d)\n", resp.Msg.GetAssignmentEpoch())
		return nil
	})
}

func cmdNodes(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return errors.New("usage: reflwd cluster nodes list [flags]")
	}
	fs := flag.NewFlagSet("nodes list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
		resp, err := cli.Cluster.ListNodes(ctx, connect.NewRequest(&clusterctlv1.ListNodesRequest{}))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp.Msg.GetNodes())
	})
}

func cmdPartitions(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return errors.New("usage: reflwd cluster partitions list [flags]")
	}
	fs := flag.NewFlagSet("partitions list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
		resp, err := cli.Cluster.ListPartitions(ctx, connect.NewRequest(&clusterctlv1.ListPartitionsRequest{}))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp.Msg.GetTable())
	})
}

func cmdSnapshot(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: reflwd cluster snapshot {create|list|delete} [flags]")
	}
	sub := args[0]
	switch sub {
	case "create":
		return cmdSnapshotCreate(ctx, args[1:])
	case "list":
		return cmdSnapshotList(ctx, args[1:])
	case "delete":
		return cmdSnapshotDelete(ctx, args[1:])
	default:
		return fmt.Errorf("snapshot: unknown subcommand %q", sub)
	}
}

func cmdSnapshotCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("snapshot create", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	shard := fs.Uint64("shard", 0, "partition shard id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *shard == 0 {
		return errors.New("--shard is required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Cluster.CreateSnapshot(rctx, connect.NewRequest(&clusterctlv1.CreateSnapshotRequest{ShardId: *shard}))
		if err != nil {
			return err
		}
		fmt.Printf("snapshot ok shard=%d index=%d size=%d\n",
			resp.Msg.GetShardId(), resp.Msg.GetIndex(), resp.Msg.GetSizeBytes())
		return nil
	})
}

func cmdSnapshotList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("snapshot list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	shard := fs.Uint64("shard", 0, "partition shard id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *shard == 0 {
		return errors.New("--shard is required")
	}
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
		resp, err := cli.Cluster.ListSnapshots(ctx, connect.NewRequest(&clusterctlv1.ListSnapshotsRequest{ShardId: *shard}))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp.Msg.GetSnapshots())
	})
}

func cmdSnapshotDelete(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("snapshot delete", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	shard := fs.Uint64("shard", 0, "partition shard id (required)")
	index := fs.Uint64("index", 0, "snapshot index (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *shard == 0 {
		return errors.New("--shard is required")
	}
	if *index == 0 {
		return errors.New("--index is required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		if _, err := cli.Cluster.DeleteSnapshot(rctx, connect.NewRequest(&clusterctlv1.DeleteSnapshotRequest{
			ShardId: *shard,
			Index:   *index,
		})); err != nil {
			return err
		}
		fmt.Printf("snapshot deleted shard=%d index=%d\n", *shard, *index)
		return nil
	})
}
