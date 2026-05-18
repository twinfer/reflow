package loadgen

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/pkg/ingressclient"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// SubprocessNode is a Node backed by an out-of-process loadnode
// binary. The chaos harness uses it to SIGKILL a process and exercise
// the torn-write Pebble WAL recovery path that graceful Host.Close
// cannot reach.
//
// SubmitInvocation / DescribeInvocation / ListPartitions go over the
// Connect Ingress service the binary serves; the chaos primitives
// manipulate the child process via Kill / Close.
type SubprocessNode struct {
	cmd         *exec.Cmd
	client      *ingressclient.Client
	ingressAddr string
	raftAddr    string

	mu      sync.Mutex
	exited  bool
	exitErr error
}

// SubprocessNodeOptions configures one subprocess node.
type SubprocessNodeOptions struct {
	BinaryPath   string
	NodeID       uint64
	RaftAddr     string
	GossipAddr   string
	DeliveryAddr string
	IngressAddr  string
	DataDir      string
	PeersFlag    string // formatted "id@raft,gossip;id@raft,gossip"
	NumShards    uint64
	// StartupTimeout bounds how long we wait for the binary's "ready"
	// line on stdout before declaring boot failed.
	StartupTimeout time.Duration
	// Stderr, if non-nil, receives the child's stderr (e.g. t.Logf).
	Stderr io.Writer
}

// startSubprocessNode spawns the loadnode binary, waits for it to log
// "loadnode: ready" on stdout, and dials its ingress address.
// On any failure the process is torn down and the error is returned.
func startSubprocessNode(opts SubprocessNodeOptions) (*SubprocessNode, error) {
	if opts.StartupTimeout <= 0 {
		opts.StartupTimeout = 30 * time.Second
	}
	cmd := exec.Command(opts.BinaryPath,
		"-node-id", fmt.Sprintf("%d", opts.NodeID),
		"-raft-addr", opts.RaftAddr,
		"-gossip-addr", opts.GossipAddr,
		"-delivery-addr", opts.DeliveryAddr,
		"-ingress-addr", opts.IngressAddr,
		"-data-dir", opts.DataDir,
		"-peers", opts.PeersFlag,
		"-num-shards", fmt.Sprintf("%d", opts.NumShards),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	if opts.Stderr != nil {
		cmd.Stderr = opts.Stderr
	}
	// Put the child in its own process group so SIGKILL on the leader
	// doesn't accidentally kill siblings, and so Wait can reap it
	// cleanly when the test tears down.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start: %w", err)
	}

	readyCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 4096)
		got := strings.Builder{}
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				got.Write(buf[:n])
				if strings.Contains(got.String(), "loadnode: ready") {
					readyCh <- nil
					// Drain any remaining stdout so the pipe doesn't block.
					_, _ = io.Copy(io.Discard, stdout)
					return
				}
			}
			if err != nil {
				readyCh <- fmt.Errorf("stdout closed before ready: %w (got: %q)", err, got.String())
				return
			}
		}
	}()

	select {
	case err := <-readyCh:
		if err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return nil, err
		}
	case <-time.After(opts.StartupTimeout):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("timeout waiting for ready (%s)", opts.StartupTimeout)
	}

	cli, err := ingressclient.Dial(ingressclient.Options{BaseURL: "http://" + opts.IngressAddr})
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("dial ingress: %w", err)
	}

	n := &SubprocessNode{
		cmd:         cmd,
		client:      cli,
		ingressAddr: opts.IngressAddr,
		raftAddr:    opts.RaftAddr,
	}
	go n.reap()
	return n, nil
}

// reap waits on cmd.Wait once and stores the exit status. Lets
// concurrent Close / Kill callers learn the process is already gone
// without racing on Wait.
func (n *SubprocessNode) reap() {
	err := n.cmd.Wait()
	n.mu.Lock()
	n.exited = true
	n.exitErr = err
	n.mu.Unlock()
}

// SubmitInvocation routes through Ingress.SubmitInvocation. The server
// mints the invocation id and routes to the destination shard via its
// Partitioner.
func (n *SubprocessNode) SubmitInvocation(ctx context.Context, service, handler, objectKey string, input []byte) (*enginev1.InvocationId, error) {
	resp, err := n.client.SubmitInvocation(ctx, connect.NewRequest(&ingressv1.SubmitInvocationRequest{
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
func (n *SubprocessNode) DescribeInvocation(ctx context.Context, id *enginev1.InvocationId) (*enginev1.InvocationStatus, error) {
	resp, err := n.client.DescribeInvocation(ctx, connect.NewRequest(&ingressv1.DescribeInvocationRequest{
		InvocationIdProto: id,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetStatus(), nil
}

// ListPartitions queries Ingress.ListPartitions and projects the proto
// into the loadgen-local shape.
func (n *SubprocessNode) ListPartitions(ctx context.Context) ([]PartitionInfo, error) {
	resp, err := n.client.ListPartitions(ctx, connect.NewRequest(&ingressv1.ListPartitionsRequest{}))
	if err != nil {
		return nil, err
	}
	out := make([]PartitionInfo, 0, len(resp.Msg.GetPartitions()))
	for _, p := range resp.Msg.GetPartitions() {
		out = append(out, PartitionInfo{
			ShardID:     p.GetShardId(),
			IsLeader:    p.GetIsLeader(),
			LeaderEpoch: p.GetLeaderEpoch(),
		})
	}
	return out, nil
}

// RaftAddr returns the raft endpoint the child binary binds.
func (n *SubprocessNode) RaftAddr() string { return n.raftAddr }

// Close gracefully tears the subprocess down: SIGTERM, wait up to 10s,
// then SIGKILL fallback. Idempotent.
func (n *SubprocessNode) Close() {
	if n == nil {
		return
	}
	_ = n.client.Close()
	n.mu.Lock()
	exited := n.exited
	n.mu.Unlock()
	if exited {
		return
	}
	if n.cmd.Process != nil {
		_ = n.cmd.Process.Signal(syscall.SIGTERM)
	}
	if !n.waitFor(10 * time.Second) {
		_ = n.cmd.Process.Kill()
		n.waitFor(2 * time.Second)
	}
}

// Kill terminates the subprocess abruptly with SIGKILL — bypasses
// graceful shutdown so Pebble's WAL is not flushed. This is the chaos
// primitive the in-process Node cannot match.
func (n *SubprocessNode) Kill() {
	if n == nil {
		return
	}
	_ = n.client.Close()
	n.mu.Lock()
	exited := n.exited
	n.mu.Unlock()
	if exited {
		return
	}
	if n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
	}
	n.waitFor(5 * time.Second)
}

// waitFor polls the reaped flag for up to dur. Returns true if the
// process has exited.
func (n *SubprocessNode) waitFor(dur time.Duration) bool {
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		exited := n.exited
		n.mu.Unlock()
		if exited {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// ExitError returns the child's exit error, or nil if the child is
// still running. Used by tests that want to assert how a node died.
func (n *SubprocessNode) ExitError() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.exited {
		return errors.New("subprocess still running")
	}
	return n.exitErr
}
