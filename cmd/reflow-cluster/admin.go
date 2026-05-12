package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/twinfer/reflow/pkg/reflow/admin"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
)

// addTLSFlags installs --client-cert / --client-key / --ca with env fallbacks.
type tlsFlags struct {
	clientCert string
	clientKey  string
	ca         string
	addr       string
}

func registerTLSFlags(fs *flag.FlagSet) *tlsFlags {
	f := &tlsFlags{}
	fs.StringVar(&f.clientCert, "client-cert", os.Getenv("REFLOW_CLIENT_CERT"), "operator cert PEM (env REFLOW_CLIENT_CERT)")
	fs.StringVar(&f.clientKey, "client-key", os.Getenv("REFLOW_CLIENT_KEY"), "operator key PEM (env REFLOW_CLIENT_KEY)")
	fs.StringVar(&f.ca, "ca", os.Getenv("REFLOW_CA_CERT"), "node CA PEM (env REFLOW_CA_CERT)")
	fs.StringVar(&f.addr, "admin", os.Getenv("REFLOW_ADMIN_ADDR"), "admin gRPC host:port (env REFLOW_ADMIN_ADDR)")
	return f
}

func (t *tlsFlags) validate() error {
	if t.addr == "" || t.clientCert == "" || t.clientKey == "" || t.ca == "" {
		return errors.New("--admin, --client-cert, --client-key, and --ca are required (or set the matching env vars)")
	}
	return nil
}

func (t *tlsFlags) dial(ctx context.Context) (*admin.Client, error) {
	return admin.Dial(ctx, admin.DialOptions{
		Addr:             t.addr,
		OperatorCertFile: t.clientCert,
		OperatorKeyFile:  t.clientKey,
		NodeCAFile:       t.ca,
	})
}

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
	if err := tls.validate(); err != nil {
		return err
	}
	if *nodeID == 0 || *raftAddr == "" || *gossipAddr == "" || *grpcEndpoint == "" {
		return errors.New("--node-id, --raft-addr, --gossip-addr, --grpc-endpoint are required")
	}
	cli, err := tls.dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()
	resp, err := cli.Admin.AddNode(ctx, &adminv1.AddNodeRequest{
		NodeId:       *nodeID,
		RaftAddr:     *raftAddr,
		GossipAddr:   *gossipAddr,
		GrpcEndpoint: *grpcEndpoint,
		NodeHostId:   *nhID,
	})
	if err != nil {
		return err
	}
	fmt.Printf("AddNode ok (assignment_epoch=%d)\n", resp.GetAssignmentEpoch())
	return nil
}

func cmdRemoveNode(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("remove-node", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	nodeID := fs.Uint64("node-id", 0, "node ID (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := tls.validate(); err != nil {
		return err
	}
	if *nodeID == 0 {
		return errors.New("--node-id is required")
	}
	cli, err := tls.dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()
	resp, err := cli.Admin.RemoveNode(ctx, &adminv1.RemoveNodeRequest{NodeId: *nodeID})
	if err != nil {
		return err
	}
	fmt.Printf("RemoveNode ok (assignment_epoch=%d)\n", resp.GetAssignmentEpoch())
	return nil
}

func cmdNodes(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return errors.New("usage: reflow-cluster nodes list [flags]")
	}
	fs := flag.NewFlagSet("nodes list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if err := tls.validate(); err != nil {
		return err
	}
	cli, err := tls.dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()
	resp, err := cli.Admin.ListNodes(ctx, &adminv1.ListNodesRequest{})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(resp.GetNodes())
}

func cmdPartitions(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return errors.New("usage: reflow-cluster partitions list [flags]")
	}
	fs := flag.NewFlagSet("partitions list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if err := tls.validate(); err != nil {
		return err
	}
	cli, err := tls.dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()
	resp, err := cli.Admin.ListPartitions(ctx, &adminv1.ListPartitionsRequest{})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(resp.GetTable())
}

func cmdSnapshot(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: reflow-cluster snapshot {create|list} [flags]")
	}
	sub := args[0]
	switch sub {
	case "create":
		return cmdSnapshotCreate(ctx, args[1:])
	case "list":
		return cmdSnapshotList(ctx, args[1:])
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
	if err := tls.validate(); err != nil {
		return err
	}
	if *shard == 0 {
		return errors.New("--shard is required")
	}
	cli, err := tls.dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()
	resp, err := cli.Admin.CreateSnapshot(ctx, &adminv1.CreateSnapshotRequest{ShardId: *shard})
	if err != nil {
		return err
	}
	fmt.Printf("snapshot ok shard=%d index=%d size=%d\n",
		resp.GetShardId(), resp.GetIndex(), resp.GetSizeBytes())
	return nil
}

func cmdSnapshotList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("snapshot list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	shard := fs.Uint64("shard", 0, "partition shard id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := tls.validate(); err != nil {
		return err
	}
	if *shard == 0 {
		return errors.New("--shard is required")
	}
	cli, err := tls.dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()
	resp, err := cli.Admin.ListSnapshots(ctx, &adminv1.ListSnapshotsRequest{ShardId: *shard})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(resp.GetSnapshots())
}

func cmdVersionBarrier(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "set" {
		return errors.New("usage: reflow-cluster version-barrier set --version=V [flags]")
	}
	fs := flag.NewFlagSet("version-barrier set", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	version := fs.Uint64("version", 0, "barrier version (required)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if err := tls.validate(); err != nil {
		return err
	}
	cli, err := tls.dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()
	resp, err := cli.Admin.SetVersionBarrier(ctx, &adminv1.SetVersionBarrierRequest{Version: *version})
	if err != nil {
		return err
	}
	fmt.Printf("version-barrier=%d assignment_epoch=%d\n",
		resp.GetVersion(), resp.GetAssignmentEpoch())
	return nil
}
