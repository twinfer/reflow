package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// ErrNotLeader is returned by Client.Send when the receiver replied with
// NotLeader. Callers should re-resolve the destination shard's leader via
// gossip and retry.
var ErrNotLeader = errors.New("delivery: receiver is not leader")

// EndpointResolver maps a destination shard id to (leaderNodeID, gRPC
// endpoint). Returning ok=false means "no current leader known"; callers
// back off and retry. *engine.Host satisfies this with the pair
// PartitionLeaderHint + NodeEndpoint.
type EndpointResolver interface {
	// PartitionLeaderHint returns the believed leader's NodeID for the
	// given partition shard.
	PartitionLeaderHint(shardID uint64) (uint64, bool)
	// NodeEndpoint returns the reflow Delivery gRPC endpoint for the
	// given NodeID, sourced from gossip Meta.
	NodeEndpoint(nodeID uint64) (string, bool)
}

// ClientConfig collects the small surface of tunables. SendTimeout
// bounds a single round trip. Dialing is non-blocking under grpc-go
// (grpc.NewClient never waits for a connection); the first Send is
// what surfaces unreachable endpoints, gated by SendTimeout.
type ClientConfig struct {
	Resolver    EndpointResolver
	Log         *slog.Logger
	SendTimeout time.Duration
	DialOptions []grpc.DialOption // overrides; defaults to insecure for in-cluster.
}

// Client is a pooled bidi-stream client for the Delivery service. Each
// destination endpoint shares a single grpc.ClientConn; sends serialize
// per endpoint behind a mutex so the request/response correlation stays
// trivially one-to-one (echoed by the seq field on the wire).
//
// Favors correctness over throughput: a single in-flight send per
// endpoint avoids interleaving concerns. Pipelining can be added if it
// becomes a bottleneck.
type Client struct {
	cfg ClientConfig

	mu    sync.Mutex
	conns map[string]*conn // keyed by endpoint
}

type conn struct {
	cc     *grpc.ClientConn
	sendMu sync.Mutex
}

// NewClient builds a Client. Resolver must be non-nil; the other fields
// are filled in with sensible defaults.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Resolver == nil {
		return nil, errors.New("delivery: ClientConfig.Resolver is required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.SendTimeout == 0 {
		cfg.SendTimeout = 5 * time.Second
	}
	if len(cfg.DialOptions) == 0 {
		cfg.DialOptions = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}
	return &Client{cfg: cfg, conns: make(map[string]*conn)}, nil
}

// Send delivers a single envelope to destShardID's current leader. On
// NotLeader the call returns ErrNotLeader and the caller is expected to
// back off + retry (the OutboxService loop handles this).
func (c *Client) Send(ctx context.Context, destShardID uint64, producerID string, seq uint64, cmd *enginev1.Command) error {
	leaderID, ok := c.cfg.Resolver.PartitionLeaderHint(destShardID)
	if !ok {
		return fmt.Errorf("delivery: no leader known for shard %d", destShardID)
	}
	endpoint, ok := c.cfg.Resolver.NodeEndpoint(leaderID)
	if !ok || endpoint == "" {
		return fmt.Errorf("delivery: no endpoint for node %d", leaderID)
	}

	co, err := c.dial(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("delivery: dial %s: %w", endpoint, err)
	}

	callCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, c.cfg.SendTimeout)
		defer cancel()
	}

	// Serialize per-endpoint so the seq echo correlation stays simple.
	co.sendMu.Lock()
	defer co.sendMu.Unlock()

	cli := deliveryv1.NewDeliveryClient(co.cc)
	stream, err := cli.Deliver(callCtx)
	if err != nil {
		return fmt.Errorf("delivery: open stream: %w", err)
	}

	if err := stream.Send(&deliveryv1.DeliverRequest{
		ShardId:    destShardID,
		ProducerId: producerID,
		Seq:        seq,
		Command:    cmd,
	}); err != nil {
		_ = stream.CloseSend()
		return fmt.Errorf("delivery: send: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("delivery: close send: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("delivery: recv: %w", err)
	}
	if resp.GetSeq() != seq {
		return fmt.Errorf("delivery: seq mismatch: got %d want %d", resp.GetSeq(), seq)
	}
	switch kind := resp.GetKind().(type) {
	case *deliveryv1.DeliverResponse_Ack:
		return nil
	case *deliveryv1.DeliverResponse_NotLeader:
		return fmt.Errorf("%w (hint=%d)", ErrNotLeader, kind.NotLeader.GetLeaderNodeId())
	case *deliveryv1.DeliverResponse_Err:
		return fmt.Errorf("delivery: receiver err: %s", kind.Err.GetMessage())
	default:
		return fmt.Errorf("delivery: unexpected response kind %T", kind)
	}
}

// dial returns the pooled connection for endpoint, creating one on first
// use. grpc.NewClient is non-blocking — the first Send is what surfaces
// an unreachable endpoint. Connections live until Close; the test for
// staleness is left to gRPC's built-in keepalive (defaults are fine for
// in-cluster RPCs).
//
// We prefix the target with the passthrough resolver scheme: reflow's
// endpoints are bare host:port strings (or test-only bufconn names like
// "bufnet") published over gossip; we don't want grpc.NewClient's default
// DNS resolver parsing or rejecting them. Connection establishment still
// goes through whatever WithContextDialer / TLS the caller configured.
func (c *Client) dial(_ context.Context, endpoint string) (*conn, error) {
	c.mu.Lock()
	if existing, ok := c.conns[endpoint]; ok {
		c.mu.Unlock()
		return existing, nil
	}
	c.mu.Unlock()

	cc, err := grpc.NewClient("passthrough:///"+endpoint, c.cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Re-check: another goroutine may have raced us to the same endpoint.
	if existing, ok := c.conns[endpoint]; ok {
		_ = cc.Close()
		return existing, nil
	}
	co := &conn{cc: cc}
	c.conns[endpoint] = co
	return co, nil
}

// Close releases all pooled gRPC connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for _, co := range c.conns {
		if err := co.cc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.conns = nil
	return firstErr
}
