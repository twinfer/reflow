package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/twinfer/reflow/pkg/reflow/admin"
	"github.com/twinfer/reflow/pkg/reflow/creds"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
)

// addTLSFlags installs --client-cert / --client-key / --ca / --trust-domain
// with env fallbacks.
type tlsFlags struct {
	clientCert  string
	clientKey   string
	ca          string
	addr        string
	trustDomain string
}

func registerTLSFlags(fs *flag.FlagSet) *tlsFlags {
	f := &tlsFlags{}
	fs.StringVar(&f.clientCert, "client-cert", os.Getenv("REFLOW_CLIENT_CERT"), "operator cert PEM (env REFLOW_CLIENT_CERT)")
	fs.StringVar(&f.clientKey, "client-key", os.Getenv("REFLOW_CLIENT_KEY"), "operator key PEM (env REFLOW_CLIENT_KEY)")
	fs.StringVar(&f.ca, "ca", os.Getenv("REFLOW_CA_CERT"), "cluster CA PEM (env REFLOW_CA_CERT)")
	fs.StringVar(&f.addr, "admin", os.Getenv("REFLOW_ADMIN_ADDR"), "admin gRPC host:port of any cluster node — mutating RPCs follow LeaderHint redirects (env REFLOW_ADMIN_ADDR)")
	fs.StringVar(&f.trustDomain, "trust-domain", envOrDefault("REFLOW_TRUST_DOMAIN", "reflow.local"), "SPIFFE trust domain (env REFLOW_TRUST_DOMAIN)")
	return f
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (t *tlsFlags) validate() error {
	if t.addr == "" || t.clientCert == "" || t.clientKey == "" || t.ca == "" {
		return errors.New("--admin, --client-cert, --client-key, and --ca are required (or set the matching env vars)")
	}
	return nil
}

func (t *tlsFlags) dialOpts() admin.DialOptions {
	return admin.DialOptions{
		Addr: t.addr,
		Creds: creds.Spec{
			Driver: creds.DriverTLS,
			TLS: &creds.TLSSpec{
				CAFile:      t.ca,
				CertFile:    t.clientCert,
				KeyFile:     t.clientKey,
				TrustDomain: t.trustDomain,
			},
		},
	}
}

func (t *tlsFlags) dial(ctx context.Context) (*admin.Client, error) {
	return admin.Dial(ctx, t.dialOpts())
}

// withClient validates the registered TLS flags, dials the admin
// endpoint, and invokes fn with the live client. Used by read-only
// subcommands (ListNodes, ListPartitions, ListSnapshots) where any
// node — leader or follower — can answer via SyncRead.
func (t *tlsFlags) withClient(ctx context.Context, fn func(*admin.Client) error) error {
	if err := t.validate(); err != nil {
		return err
	}
	cli, err := t.dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()
	return fn(cli)
}

// withLeaderRedirect validates the registered TLS flags and invokes fn
// inside admin.CallWithLeaderRedirect — the configured --admin can be
// any cluster node; on codes.Unavailable + LeaderHint the wrapper
// redials the hinted endpoint. Used by mutating subcommands (AddNode,
// RemoveNode, CreateSnapshot, DeleteSnapshot, RegisterDeployment)
// which must reach the metadata leader.
func (t *tlsFlags) withLeaderRedirect(
	ctx context.Context,
	fn func(context.Context, adminv1.AdminClient) error,
) error {
	if err := t.validate(); err != nil {
		return err
	}
	return admin.CallWithLeaderRedirect(ctx, t.dialOpts(), 3, fn)
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
	if *nodeID == 0 || *raftAddr == "" || *gossipAddr == "" || *grpcEndpoint == "" {
		return errors.New("--node-id, --raft-addr, --gossip-addr, --grpc-endpoint are required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli adminv1.AdminClient) error {
		resp, err := cli.AddNode(rctx, &adminv1.AddNodeRequest{
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
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli adminv1.AdminClient) error {
		resp, err := cli.RemoveNode(rctx, &adminv1.RemoveNodeRequest{NodeId: *nodeID})
		if err != nil {
			return err
		}
		fmt.Printf("RemoveNode ok (assignment_epoch=%d)\n", resp.GetAssignmentEpoch())
		return nil
	})
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
	return tls.withClient(ctx, func(cli *admin.Client) error {
		resp, err := cli.Admin.ListNodes(ctx, &adminv1.ListNodesRequest{})
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp.GetNodes())
	})
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
	return tls.withClient(ctx, func(cli *admin.Client) error {
		resp, err := cli.Admin.ListPartitions(ctx, &adminv1.ListPartitionsRequest{})
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp.GetTable())
	})
}

func cmdSnapshot(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: reflow-cluster snapshot {create|list|delete} [flags]")
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
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli adminv1.AdminClient) error {
		resp, err := cli.CreateSnapshot(rctx, &adminv1.CreateSnapshotRequest{ShardId: *shard})
		if err != nil {
			return err
		}
		fmt.Printf("snapshot ok shard=%d index=%d size=%d\n",
			resp.GetShardId(), resp.GetIndex(), resp.GetSizeBytes())
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
	return tls.withClient(ctx, func(cli *admin.Client) error {
		resp, err := cli.Admin.ListSnapshots(ctx, &adminv1.ListSnapshotsRequest{ShardId: *shard})
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp.GetSnapshots())
	})
}

func cmdRegisterDeployment(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("register-deployment", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	rawURL := fs.String("url", "", "handler deployment URL (http:// or https://)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *rawURL == "" {
		return errors.New("--url is required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli adminv1.AdminClient) error {
		resp, err := cli.RegisterDeployment(rctx, &adminv1.RegisterDeploymentRequest{Url: *rawURL})
		if err != nil {
			return err
		}
		fmt.Printf("RegisterDeployment ok (deployment_id=%s)\n", resp.GetDeploymentId())
		return nil
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
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli adminv1.AdminClient) error {
		if _, err := cli.DeleteSnapshot(rctx, &adminv1.DeleteSnapshotRequest{
			ShardId: *shard,
			Index:   *index,
		}); err != nil {
			return err
		}
		fmt.Printf("snapshot deleted shard=%d index=%d\n", *shard, *index)
		return nil
	})
}
