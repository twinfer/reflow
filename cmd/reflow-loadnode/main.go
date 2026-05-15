// Command reflow-loadnode is a minimal reflow node used by the chaos
// test harness. It spawns one engine.Host wired the same way the
// in-process test cluster (internal/loadgen) wires its nodes, registers
// loadgen.HelloHandler, serves the Delivery + Ingress gRPC services,
// and blocks on signals.
//
// This binary exists so chaos tests can SIGKILL a process to exercise
// torn-write Pebble WAL recovery — something graceful Host.Close
// cannot exercise. Production deployments use cmd/reflowd, not this.
//
// Flags mirror the addresses internal/loadgen.NewCluster allocates per
// node; the test spawns one process per cluster member and threads the
// pre-allocated addresses through.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/delivery"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/sdk"
	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "reflow-loadnode: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		nodeID       uint64
		raftAddr     string
		gossipAddr   string
		deliveryAddr string
		ingressAddr  string
		dataDir      string
		peersFlag    string
		numShards    uint64
		joinExisting bool
	)
	flag.Uint64Var(&nodeID, "node-id", 0, "raft node id (1..N)")
	flag.StringVar(&raftAddr, "raft-addr", "", "raft transport address (host:port)")
	flag.StringVar(&gossipAddr, "gossip-addr", "", "gossip address (host:port)")
	flag.StringVar(&deliveryAddr, "delivery-addr", "", "cross-shard Delivery gRPC address (host:port)")
	flag.StringVar(&ingressAddr, "ingress-addr", "", "Ingress gRPC address served by this node (host:port)")
	flag.StringVar(&dataDir, "data-dir", "", "on-disk dataDir for Pebble + raft log")
	flag.StringVar(&peersFlag, "peers", "", "comma-sep peers: id@raft,gossip (one entry per cluster member, including self)")
	flag.Uint64Var(&numShards, "num-shards", 0, "number of partition shards (1..N)")
	flag.BoolVar(&joinExisting, "join", false, "join an already-running cluster (StartOnDiskReplica with join=true); admin AddNode must have already added this NodeID")
	flag.Parse()

	if nodeID == 0 || raftAddr == "" || gossipAddr == "" || deliveryAddr == "" ||
		ingressAddr == "" || dataDir == "" || peersFlag == "" || numShards == 0 {
		return fmt.Errorf("required flag missing: see -help")
	}

	peers, err := parsePeers(peersFlag)
	if err != nil {
		return fmt.Errorf("parse peers: %w", err)
	}

	reg := sdk.NewRegistry()
	if err := reg.Register("loadgen.Hello", "echo", loadgen.HelloHandler); err != nil {
		return fmt.Errorf("register handler: %w", err)
	}

	host, err := engine.NewHost(engine.HostConfig{
		NodeID:             nodeID,
		RaftAddr:           raftAddr,
		DataDir:            dataDir,
		RTTMillisecond:     50,
		NumPartitionShards: numShards,
		Handlers:           reg,
		Peers:              peers,
		GossipBindAddr:     gossipAddr,
		GossipAdvAddr:      gossipAddr,
		GrpcEndpoint:       deliveryAddr,
		JoinExisting:       joinExisting,
	})
	if err != nil {
		return fmt.Errorf("engine.NewHost: %w", err)
	}
	defer host.Close()

	dc, err := delivery.NewClient(delivery.ClientConfig{Resolver: host})
	if err != nil {
		return fmt.Errorf("delivery.NewClient: %w", err)
	}
	defer dc.Close()
	host.SetCrossShardSender(dc)

	dln, err := listenWithRetry(deliveryAddr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("listen delivery: %w", err)
	}
	dgs := grpc.NewServer()
	deliveryv1.RegisterDeliveryServer(dgs, delivery.NewServer(host, nil))
	go func() {
		if err := dgs.Serve(dln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			fmt.Fprintf(os.Stderr, "reflow-loadnode: delivery Serve exited: %v\n", err)
		}
	}()
	defer dgs.GracefulStop()

	if _, err := host.StartMetadataShard(); err != nil {
		return fmt.Errorf("StartMetadataShard: %w", err)
	}
	for sh := uint64(1); sh <= numShards; sh++ {
		if _, err := host.StartPartition(sh); err != nil {
			return fmt.Errorf("StartPartition(%d): %w", sh, err)
		}
	}

	iln, err := listenWithRetry(ingressAddr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("listen ingress: %w", err)
	}
	igs := grpc.NewServer()
	ingressv1.RegisterIngressServer(igs, ingress.NewServer(host, nil))
	go func() {
		if err := igs.Serve(iln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			fmt.Fprintf(os.Stderr, "reflow-loadnode: ingress Serve exited: %v\n", err)
		}
	}()
	defer igs.GracefulStop()

	// Tell the parent process we're serving. The test waits for this
	// line on stdout before connecting its ingress client.
	fmt.Println("reflow-loadnode: ready")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	return nil
}

// parsePeers reads "id@raft,gossip;id@raft,gossip;..." or
// "id@raft,gossip|id@raft,gossip|..." into a []engine.Peer. Commas
// inside the host:port fields force the outer separator to be ';' or
// '|' — the harness uses ';'.
func parsePeers(s string) ([]engine.Peer, error) {
	if s == "" {
		return nil, fmt.Errorf("empty peers")
	}
	sep := ";"
	if !strings.Contains(s, sep) && strings.Contains(s, "|") {
		sep = "|"
	}
	entries := strings.Split(s, sep)
	out := make([]engine.Peer, 0, len(entries))
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		atIdx := strings.IndexByte(e, '@')
		if atIdx <= 0 {
			return nil, fmt.Errorf("malformed peer %q: missing '@'", e)
		}
		id, err := strconv.ParseUint(e[:atIdx], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("malformed peer %q: id: %w", e, err)
		}
		rest := e[atIdx+1:]
		commaIdx := strings.IndexByte(rest, ',')
		if commaIdx <= 0 {
			return nil, fmt.Errorf("malformed peer %q: missing ',' between raft and gossip", e)
		}
		out = append(out, engine.Peer{
			NodeID:     id,
			RaftAddr:   strings.TrimSpace(rest[:commaIdx]),
			GossipAddr: strings.TrimSpace(rest[commaIdx+1:]),
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no peers parsed")
	}
	return out, nil
}

func listenWithRetry(addr string, timeout time.Duration) (net.Listener, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln, nil
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	return nil, lastErr
}
