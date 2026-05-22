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
	terminated bool
}

// SubmitInvocation routes through Ingress.SubmitInvocation on this node;
// the server mints the invocation id and forwards to the destination
// shard via its Partitioner. Mirrors loadgen.SubprocessNode.SubmitInvocation.
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

// ListPartitions queries Ingress.ListPartitions and projects the proto
// into the loadgen-local PartitionInfo shape so chaos primitives that
// previously consumed *loadgen.SubprocessNode work unchanged.
func (n *ContainerNode) ListPartitions(ctx context.Context) ([]loadgen.PartitionInfo, error) {
	cli, err := n.ingress()
	if err != nil {
		return nil, err
	}
	resp, err := cli.ListPartitions(ctx, connect.NewRequest(&ingressv1.ListPartitionsRequest{}))
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
