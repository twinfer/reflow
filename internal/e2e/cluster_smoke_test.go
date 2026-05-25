//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/e2e"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestSmoke_ThreeNodeClusterInvocation is the PR-3 capstone: it brings
// up a 3-node insecure reflowd cluster + a loadhandler sidecar, registers
// the deployment, submits a single invocation through one node's ingress,
// and polls until DescribeInvocation reports Completed. Exercises:
//
//   - end-to-end cluster bring-up (3 reflowd containers, network DNS,
//     gossip rendezvous, raft election, ingress + admin listeners);
//   - Config.RegisterDeployment over the admin Connect RPC;
//   - engine → handler routing across container boundaries (engine in
//     reflowd-node*, handler in the loadhandler sidecar);
//   - Ingress.SubmitInvocation + Ingress.DescribeInvocation polling.
func TestSmoke_ThreeNodeClusterInvocation(t *testing.T) {
	cluster := e2e.NewContainerCluster(t, e2e.ContainerClusterOptions{N: 3, NumShards: 1})
	handler := e2e.StartHandlerContainer(t, cluster.Net)

	regCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := e2e.RegisterHandler(regCtx, cluster, handler); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	// A just-booted node's host-mapped ingress port can flap under Docker
	// Desktop's port forwarder, and a freshly-elected cluster needs a moment
	// before the destination shard leader serves the propose. Submit through
	// any node, retrying those startup transients (see submitWhenReady).
	id, err := submitWhenReady(t, cluster, 45*time.Second)
	if err != nil {
		t.Fatalf("submit invocation: %v", err)
	}

	// Engine→handler dispatch is over h2c on the docker network; even
	// for an echo, give the cluster headroom for the first dispatch
	// (the engine's handler-client deployment-discovery + connection
	// pool warmup happens on the first call).
	if err := awaitCompletion(t, cluster, id, 60*time.Second); err != nil {
		t.Fatalf("await completion: %v", err)
	}
}

// submitWhenReady submits the smoke invocation through any node, retrying
// transient transport errors until one node accepts it or timeout elapses.
// A just-booted node's host-mapped ingress port can flap under Docker
// Desktop's port forwarder (CodeUnavailable / connection refused), and a
// freshly-elected cluster needs a moment before the destination shard
// leader serves the propose (CodeDeadlineExceeded). Both are startup
// warm-up transients, not invocation failures — round-robin across nodes
// like RegisterHandler rather than pin to one.
func submitWhenReady(t *testing.T, cluster *e2e.ContainerCluster, timeout time.Duration) (*enginev1.InvocationId, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		for _, n := range cluster.Nodes {
			if n == nil {
				continue
			}
			// Docker Desktop re-maps a running container's host port when the
			// network topology changes (the loadhandler sidecar is added after
			// the cluster is up), so re-resolve before each dial.
			if err := n.RefreshPorts(context.Background()); err != nil {
				lastErr = err
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			id, err := n.SubmitInvocation(ctx, "e2e.Echo", "echo", "smoke-1", []byte("hello"))
			cancel()
			if err == nil {
				return id, nil
			}
			lastErr = err
			if code := connect.CodeOf(err); code != connect.CodeUnavailable && code != connect.CodeDeadlineExceeded {
				return nil, err
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return nil, fmt.Errorf("no node accepted the invocation within %s: %w", timeout, lastErr)
}

// awaitCompletion polls DescribeInvocation against any reachable node
// until the status reaches the Completed terminal state, or the
// deadline expires. A non-empty failure_message surfaces as an error
// for diagnosis instead of being treated as success.
func awaitCompletion(t *testing.T, cluster *e2e.ContainerCluster, id *enginev1.InvocationId, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastDesc string
	for time.Now().Before(deadline) {
		for _, n := range cluster.Nodes {
			if n == nil {
				continue
			}
			// Host ports can have been re-mapped since construction (see
			// submitWhenReady); re-resolve so polling doesn't dial a dead port.
			if err := n.RefreshPorts(context.Background()); err != nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			st, err := n.DescribeInvocation(ctx, id)
			cancel()
			if err != nil || st == nil {
				continue
			}
			lastDesc = describe(st)
			done := st.GetCompleted()
			if done == nil {
				continue
			}
			if msg := done.GetFailureMessage(); msg != "" {
				return fmt.Errorf("invocation failed: code=%d msg=%s", done.GetFailureCode(), msg)
			}
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastDesc == "" {
		return fmt.Errorf("timed out waiting for completion (no DescribeInvocation response)")
	}
	return fmt.Errorf("timed out waiting for completion (last status: %s)", lastDesc)
}

// describe returns a short string showing which oneof variant the
// status holds (Free / Scheduled / Invoked / Suspended / Completed).
// Used for diagnostic context on timeout.
func describe(st *enginev1.InvocationStatus) string {
	switch {
	case st.GetCompleted() != nil:
		return "Completed"
	case st.GetSuspended() != nil:
		return "Suspended"
	case st.GetInvoked() != nil:
		return "Invoked"
	case st.GetScheduled() != nil:
		return "Scheduled"
	case st.GetFree() != nil:
		return "Free"
	default:
		return "<unknown>"
	}
}
