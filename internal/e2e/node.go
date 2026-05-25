//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"

	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/ingressclient"
	"github.com/twinfer/reflow/pkg/reflowclient"
	clusterctlv1 "github.com/twinfer/reflow/proto/clusterctlv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// ContainerNode is one reflowd container plus an ingress client dialed
// against its host-mapped ingress port. Implements loadgen.Node so the
// workload + invariants helpers in internal/loadgen work against the
// containerized cluster unchanged. The interface is verified by the
// compile-time check at the bottom of this file.
type ContainerNode struct {
	nodeID          uint64
	raftAddr        string // docker-internal advertised value (host:port)
	adminEndpoint   string // docker-internal admin advertised value (host:port)
	ingressURL      string // http://localhost:<mapped> — used by ingressCli
	adminURLForTest string // http://localhost:<mapped> — used by RegisterHandler

	mu         sync.Mutex
	container  testcontainers.Container
	ingressCli *ingressclient.Client
	// terminated is true after Close — the container has been removed
	// from the docker daemon and cannot be restarted. killed is true
	// between Kill and the next successful Restart — the container
	// exists but its main process is dead, so AnyLiveNode skips it.
	// paused is true between Pause and Unpause — the kernel has frozen
	// the cgroup but the process is intact, so the data dir is still
	// consistent on resume and chaos primitives that need a "node is
	// alive but not responding" shape can synthesize it here.
	terminated bool
	killed     bool
	paused     bool
}

// SubmitInvocation routes through Ingress.SubmitInvocation on this node;
// the server mints the invocation id and forwards to the destination
// shard via its Partitioner.
func (n *ContainerNode) SubmitInvocation(ctx context.Context, service, handler, objectKey string, input []byte) (*enginev1.InvocationId, error) {
	cli, err := n.ingress()
	if err != nil {
		return nil, err
	}
	resp, err := cli.SubmitInvocation(ctx, connect.NewRequest(&ingressv1.SubmitInvocationRequest{
		Service:   service,
		Handler:   handler,
		ObjectKey: objectKey,
		Input:     input,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetInvocationId(), nil
}

// DescribeInvocation queries the non-blocking ingress endpoint.
func (n *ContainerNode) DescribeInvocation(ctx context.Context, id *enginev1.InvocationId) (*enginev1.InvocationStatus, error) {
	cli, err := n.ingress()
	if err != nil {
		return nil, err
	}
	resp, err := cli.DescribeInvocation(ctx, connect.NewRequest(&ingressv1.DescribeInvocationRequest{
		InvocationIdProto: id,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetStatus(), nil
}

// ListPartitions queries ClusterCtl.NodeLeadership on this node's admin
// port and projects the proto into the loadgen-local PartitionInfo shape so
// chaos primitives that drive the loadgen.Node surface work unchanged. The
// node-local live-leadership read moved off the public ingress plane onto
// the operator admin surface (see ClusterCtl/NodeLeadership), so this dials
// the admin URL rather than the ingress client.
func (n *ContainerNode) ListPartitions(ctx context.Context) ([]loadgen.PartitionInfo, error) {
	cli, err := reflowclient.Dial(ctx, reflowclient.DialOptions{Addr: stripScheme(n.AdminURLForTest())})
	if err != nil {
		return nil, err
	}
	defer cli.Close()
	resp, err := cli.Cluster.NodeLeadership(ctx, connect.NewRequest(&clusterctlv1.NodeLeadershipRequest{}))
	if err != nil {
		return nil, err
	}
	parts := resp.Msg.GetPartitions()
	out := make([]loadgen.PartitionInfo, 0, len(parts))
	for _, p := range parts {
		out = append(out, loadgen.PartitionInfo{
			ShardID:     p.GetShardId(),
			IsLeader:    p.GetIsLeader(),
			LeaderEpoch: p.GetLeaderEpoch(),
		})
	}
	return out, nil
}

// RaftAddr returns the address dragonboat advertises for this node
// inside the docker network. Used by chaos primitives keyed by raft
// endpoint (matrix Cut/Heal etc.).
func (n *ContainerNode) RaftAddr() string { return n.raftAddr }

// NodeID exposes the static cluster member ID. Not part of loadgen.Node
// but used by the e2e harness when constructing peer-keyed maps.
func (n *ContainerNode) NodeID() uint64 { return n.nodeID }

// AdminEndpoint returns the docker-internal admin advertise value
// (e.g. "reflowd-node1:8082"). Used by the cluster helper that picks
// the seed admin endpoint for RegisterDeployment.
func (n *ContainerNode) AdminEndpoint() string { return n.adminEndpoint }

// AdminURLForTest returns the host-mapped admin URL the test process
// can dial from outside the docker network. RegisterHandler uses this
// when calling Config.RegisterDeployment over operator-mTLS (or h2c
// for insecure clusters).
func (n *ContainerNode) AdminURLForTest() string { return n.adminURLForTest }

// IsTerminated reports whether Close has fully removed this node from
// the daemon. After Close the node is unrecoverable; chaos primitives
// SKIP terminated nodes in AnyLiveNode.
func (n *ContainerNode) IsTerminated() bool {
	if n == nil {
		return true
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.terminated
}

// IsLive reports whether the node currently has a running process.
// Returns false after Kill (until a successful Restart), after Pause
// (until a successful Unpause), and after Close (permanently). Used
// by ContainerCluster.AnyLiveNode to skip dead containers without
// renumbering the Nodes slice.
func (n *ContainerNode) IsLive() bool {
	if n == nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return !n.terminated && !n.killed && !n.paused
}

// Close gracefully stops + removes the container. Idempotent.
func (n *ContainerNode) Close() {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ingressCli != nil {
		_ = n.ingressCli.Close()
		n.ingressCli = nil
	}
	if n.terminated || n.container == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = testcontainers.TerminateContainer(n.container)
	_ = ctx
	n.terminated = true
}

// Kill terminates the container abruptly via Docker API ContainerKill
// with SIGKILL — bypasses any in-container graceful shutdown so the
// reflowd process exits without flushing the Pebble WAL. This is the
// chaos primitive the in-process Node cannot match.
//
// The container itself is NOT removed; the writable layer (including
// the Pebble + dragonboat data dir at /home/nonroot/reflow) persists
// so Restart can bring the node back from its on-disk state. To fully
// tear the container down, call Close.
func (n *ContainerNode) Kill() {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ingressCli != nil {
		_ = n.ingressCli.Close()
		n.ingressCli = nil
	}
	if n.terminated || n.container == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return
	}
	_, _ = cli.ContainerKill(ctx, n.container.GetContainerID(),
		dockerclient.ContainerKillOptions{Signal: "SIGKILL"})
	n.killed = true
}

// Restart re-starts a previously-killed container. The data dir
// (/home/nonroot/reflow) survives the kill+start cycle because it
// lives inside the container's writable layer and `docker start`
// reuses it. After Start succeeds the host-mapped ingress + admin
// ports may have changed (Docker re-binds on restart), so the
// internal URLs are refreshed and the ingress client is recreated
// lazily on next use.
//
// Returns nil + restarts a never-killed node (no-op safe). Returns
// an error when the container has been fully terminated via Close
// (testcontainers won't restart a removed container).
func (n *ContainerNode) Restart(ctx context.Context) error {
	if n == nil {
		return fmt.Errorf("e2e: Restart on nil node")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.terminated {
		return fmt.Errorf("e2e: Restart after Close")
	}
	if !n.killed {
		return nil
	}
	if err := n.container.Start(ctx); err != nil {
		return fmt.Errorf("e2e: Restart node %d: %w", n.nodeID, err)
	}
	// Port mappings can change across kill+start. Re-resolve so the
	// next SubmitInvocation / DescribeInvocation dial lands on the
	// fresh host-mapped port.
	host, err := n.container.Host(ctx)
	if err != nil {
		return fmt.Errorf("e2e: Restart node %d: host: %w", n.nodeID, err)
	}
	ingMap, err := n.container.MappedPort(ctx, ingressPort+"/tcp")
	if err != nil {
		return fmt.Errorf("e2e: Restart node %d: mapped ingress port: %w", n.nodeID, err)
	}
	admMap, err := n.container.MappedPort(ctx, adminPort+"/tcp")
	if err != nil {
		return fmt.Errorf("e2e: Restart node %d: mapped admin port: %w", n.nodeID, err)
	}
	n.ingressURL = "http://" + net.JoinHostPort(host, ingMap.Port())
	n.adminURLForTest = "http://" + net.JoinHostPort(host, admMap.Port())
	n.killed = false
	return nil
}

// Pause freezes the container's cgroup via Docker API ContainerPause —
// every process inside the container is suspended by SIGSTOP-style
// kernel freezing. The Pebble + dragonboat data dir is left fully
// consistent (no writes are mid-flight in user-space when the freeze
// lands), so Unpause resumes from the exact prior state. Idempotent
// when already paused; refuses when terminated.
//
// While paused the node is not Live: peers' raft heartbeats time out
// and a re-election proceeds. This is the durable analog of a
// "frozen process" — useful for testing the suspect path that
// dragonboat takes when a peer goes unreachable without dying.
func (n *ContainerNode) Pause(ctx context.Context) error {
	if n == nil {
		return fmt.Errorf("e2e: Pause on nil node")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.terminated {
		return fmt.Errorf("e2e: Pause after Close")
	}
	if n.killed {
		return fmt.Errorf("e2e: Pause on killed node")
	}
	if n.paused {
		return nil
	}
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return fmt.Errorf("e2e: Pause node %d: docker client: %w", n.nodeID, err)
	}
	if _, err := cli.ContainerPause(ctx, n.container.GetContainerID(),
		dockerclient.ContainerPauseOptions{}); err != nil {
		return fmt.Errorf("e2e: Pause node %d: %w", n.nodeID, err)
	}
	n.paused = true
	return nil
}

// Unpause resumes a previously-paused container. Port mappings are
// preserved across pause+unpause (Docker does NOT re-bind), so the
// cached ingress / admin URLs are still valid; the ingress client
// stays attached. Idempotent when not paused.
func (n *ContainerNode) Unpause(ctx context.Context) error {
	if n == nil {
		return fmt.Errorf("e2e: Unpause on nil node")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.terminated {
		return fmt.Errorf("e2e: Unpause after Close")
	}
	if !n.paused {
		return nil
	}
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return fmt.Errorf("e2e: Unpause node %d: docker client: %w", n.nodeID, err)
	}
	if _, err := cli.ContainerUnpause(ctx, n.container.GetContainerID(),
		dockerclient.ContainerUnpauseOptions{}); err != nil {
		return fmt.Errorf("e2e: Unpause node %d: %w", n.nodeID, err)
	}
	n.paused = false
	return nil
}

// ingress returns the ingress client, dialing on first use. Cached for
// the node's lifetime; recreated on Close.
func (n *ContainerNode) ingress() (*ingressclient.Client, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ingressCli != nil {
		return n.ingressCli, nil
	}
	cli, err := ingressclient.Dial(ingressclient.Options{BaseURL: n.ingressURL})
	if err != nil {
		return nil, fmt.Errorf("e2e: dial ingress %s: %w", n.ingressURL, err)
	}
	n.ingressCli = cli
	return cli, nil
}

// resolveHostMapped returns "http://<host>:<port>" for a tcp port the
// container has mapped to the host. Test code uses this to dial the
// node's ingress / admin endpoint from outside the docker network.
func resolveHostMapped(t testing.TB, ctx context.Context, c testcontainers.Container, containerPort string) string {
	t.Helper()
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("e2e: container host: %v", err)
	}
	mapped, err := c.MappedPort(ctx, containerPort)
	if err != nil {
		t.Fatalf("e2e: mapped port %s: %v", containerPort, err)
	}
	return "http://" + net.JoinHostPort(host, mapped.Port())
}

// Compile-time check: ContainerNode satisfies loadgen.Node so the
// non-loadgen harness types (Workload, AwaitCompletion, Sampler) can
// drive containerized clusters without modification.
var _ loadgen.Node = (*ContainerNode)(nil)
