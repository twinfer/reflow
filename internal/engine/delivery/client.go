package delivery

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
	"github.com/twinfer/reflow/proto/deliveryv1/deliveryv1connect"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// ErrNotLeader is returned by Client.Send when the receiver replied with
// NotLeader. Callers should re-resolve the destination shard's leader via
// gossip and retry.
var ErrNotLeader = errors.New("delivery: receiver is not leader")

// EndpointResolver maps a destination shard id to (leaderNodeID,
// endpoint). Returning ok=false means "no current leader known"; callers
// back off and retry. *engine.Host satisfies this with the pair
// PartitionLeaderHint + NodeEndpoint. The endpoint is a bare host:port
// string published over gossip; the URL scheme is derived from
// ClientConfig.ClientTLSConfig.
type EndpointResolver interface {
	// PartitionLeaderHint returns the believed leader's NodeID for the
	// given partition shard.
	PartitionLeaderHint(shardID uint64) (uint64, bool)
	// NodeEndpoint returns the reflow Delivery endpoint for the
	// given NodeID, sourced from gossip Meta.
	NodeEndpoint(nodeID uint64) (string, bool)
}

// ReplicaResolver enumerates the replicas of a partition shard. Used by
// the LP-transfer SST uploader to fan an upload across every replica
// (Pebble Ingest is replica-local). *engine.Host satisfies this via
// PartitionReplicas (one linearizable read of shard 0's PartitionTable).
type ReplicaResolver interface {
	PartitionReplicas(ctx context.Context, shardID uint64) ([]uint64, error)
	NodeEndpoint(nodeID uint64) (string, bool)
}

// ClientConfig collects the small surface of tunables. SendTimeout
// bounds a single round trip. ClientTLSConfig selects the transport:
// non-nil → https + HTTP/2 over TLS; nil → http + h2c.
type ClientConfig struct {
	Resolver        EndpointResolver
	Log             *slog.Logger
	SendTimeout     time.Duration
	ClientTLSConfig *tls.Config
	// UploadTimeout bounds a single LP-transfer SST upload. Distinct from
	// SendTimeout because uploads carry multi-MB payloads — the 5-second
	// default for Send would routinely truncate. Default 10 minutes.
	UploadTimeout time.Duration
	// UploadChunkBytes is the per-frame body size on UploadLPTransferSST.
	// Default 256 KiB — large enough to amortize per-frame overhead, small
	// enough that one frame fits comfortably in a Connect message.
	UploadChunkBytes int
	// Transport, when non-nil, replaces the http.Transport this client
	// would otherwise construct. Used by tests that need to dial a
	// non-network listener (httptest server, bufconn-like fakes).
	Transport http.RoundTripper
}

// Client is a pooled bidi-stream client for the Delivery service. Each
// destination endpoint shares a single http.Client + Connect client;
// sends serialize per endpoint behind a mutex so the request/response
// correlation stays trivially one-to-one (echoed by the seq field on the
// wire).
//
// Favors correctness over throughput: a single in-flight send per
// endpoint avoids interleaving concerns. Pipelining can be added if it
// becomes a bottleneck.
type Client struct {
	cfg ClientConfig

	mu    sync.Mutex
	conns map[string]*conn // keyed by base URL (scheme + endpoint)
}

type conn struct {
	httpc  *http.Client
	tr     http.RoundTripper // owned, may be *http.Transport for CloseIdleConnections
	client deliveryv1connect.DeliveryClient
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
	if cfg.UploadTimeout == 0 {
		cfg.UploadTimeout = 10 * time.Minute
	}
	if cfg.UploadChunkBytes <= 0 {
		cfg.UploadChunkBytes = 256 * 1024
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

	co, err := c.dial(endpoint)
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

	stream := co.client.Deliver(callCtx)
	if err := stream.Send(&deliveryv1.DeliverRequest{
		ShardId:    destShardID,
		ProducerId: producerID,
		Seq:        seq,
		Command:    cmd,
	}); err != nil {
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
		return fmt.Errorf("delivery: send: %w", err)
	}
	if err := stream.CloseRequest(); err != nil {
		_ = stream.CloseResponse()
		return fmt.Errorf("delivery: close send: %w", err)
	}

	resp, err := stream.Receive()
	closeErr := stream.CloseResponse()
	if err != nil {
		return fmt.Errorf("delivery: recv: %w", err)
	}
	if closeErr != nil {
		c.cfg.Log.Debug("delivery: close response stream", "err", closeErr)
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

// UploadLPTransferSST streams an SST file to the dest-shard replica
// hosted by targetNodeID. Returns the receiver-echoed relative path on
// ack, ErrNotLeader when the receiver does not host destShardID (used
// as a pre-flight redirect — the server accepts uploads on any replica
// that hosts the shard, since Pebble Ingest is replica-local), and an
// ordinary error otherwise. namespace + transferID must each be a
// single path segment (no separators); the server rejects malformed
// inputs.
//
// The body is read from filePath, hashed with sha256 as it streams,
// and the header carries (size_bytes, sha256_hex) for the receiver to
// verify before fsync + atomic rename. Re-upload of the same
// (transfer_id, namespace) is idempotent at the server.
//
// Caller-supplied targetNodeID so the LP-transfer fan-out wrapper can
// upload to every replica of destShardID in parallel (Pebble Ingest is
// replica-local — every replica needs the file).
func (c *Client) UploadLPTransferSST(
	ctx context.Context,
	targetNodeID uint64,
	destShardID uint64,
	transferID string,
	namespace string,
	filePath string,
) (string, error) {
	endpoint, ok := c.cfg.Resolver.NodeEndpoint(targetNodeID)
	if !ok || endpoint == "" {
		return "", fmt.Errorf("delivery upload: no endpoint for node %d", targetNodeID)
	}
	co, err := c.dial(endpoint)
	if err != nil {
		return "", fmt.Errorf("delivery upload: dial %s: %w", endpoint, err)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("delivery upload: open %s: %w", filePath, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("delivery upload: stat %s: %w", filePath, err)
	}

	// Hash separately so the header carries the full-body sha256 before
	// the receiver sees any chunk.
	sum, err := fileSHA256(filePath)
	if err != nil {
		return "", fmt.Errorf("delivery upload: hash %s: %w", filePath, err)
	}

	callCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, c.cfg.UploadTimeout)
		defer cancel()
	}

	// Serialize uploads per-endpoint behind sendMu so a fast OutboxAck
	// Send cannot interleave with a slow multi-MB upload on the same
	// HTTP/2 stream pool. Connect streams are separate HTTP/2 streams
	// in principle, but serializing keeps the failure surface trivial.
	co.sendMu.Lock()
	defer co.sendMu.Unlock()

	stream := co.client.UploadLPTransferSST(callCtx)
	hdr := &deliveryv1.UploadLPTransferSSTRequest{
		Kind: &deliveryv1.UploadLPTransferSSTRequest_Header{
			Header: &deliveryv1.UploadLPTransferSSTHeader{
				DestShard:  destShardID,
				TransferId: transferID,
				Namespace:  namespace,
				SizeBytes:  uint64(st.Size()),
				Sha256Hex:  sum,
			},
		},
	}
	if err := stream.Send(hdr); err != nil {
		_, _ = stream.CloseAndReceive()
		return "", fmt.Errorf("delivery upload: send header: %w", err)
	}

	buf := make([]byte, c.cfg.UploadChunkBytes)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			frame := &deliveryv1.UploadLPTransferSSTRequest{
				Kind: &deliveryv1.UploadLPTransferSSTRequest_Chunk{Chunk: append([]byte(nil), buf[:n]...)},
			}
			if err := stream.Send(frame); err != nil {
				_, _ = stream.CloseAndReceive()
				return "", fmt.Errorf("delivery upload: send chunk: %w", err)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_, _ = stream.CloseAndReceive()
			return "", fmt.Errorf("delivery upload: read body: %w", rerr)
		}
	}
	resp, err := stream.CloseAndReceive()
	if err != nil {
		return "", fmt.Errorf("delivery upload: close+recv: %w", err)
	}
	switch kind := resp.Msg.GetKind().(type) {
	case *deliveryv1.UploadLPTransferSSTResponse_Ack:
		return kind.Ack.GetRelativePath(), nil
	case *deliveryv1.UploadLPTransferSSTResponse_NotLeader:
		return "", fmt.Errorf("%w (hint=%d)", ErrNotLeader, kind.NotLeader.GetLeaderNodeId())
	case *deliveryv1.UploadLPTransferSSTResponse_Err:
		return "", fmt.Errorf("delivery upload: receiver err: %s", kind.Err.GetMessage())
	default:
		return "", fmt.Errorf("delivery upload: unexpected response kind %T", kind)
	}
}

// fileSHA256 hashes the file at path in one pass and returns the
// lowercase hex digest.
func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// dial returns the pooled connection for endpoint, creating one on
// first use. The base URL scheme is derived from ClientTLSConfig: https
// when set, http (h2c) otherwise. Connections live until Close.
func (c *Client) dial(endpoint string) (*conn, error) {
	baseURL := c.baseURL(endpoint)

	c.mu.Lock()
	if existing, ok := c.conns[baseURL]; ok {
		c.mu.Unlock()
		return existing, nil
	}
	c.mu.Unlock()

	tr, err := c.newTransport()
	if err != nil {
		return nil, err
	}
	hc := &http.Client{Transport: tr}
	co := &conn{
		httpc:  hc,
		tr:     tr,
		client: deliveryv1connect.NewDeliveryClient(hc, baseURL),
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.conns[baseURL]; ok {
		// Lost the race: another goroutine inserted first. Discard ours.
		closeTransport(tr)
		return existing, nil
	}
	c.conns[baseURL] = co
	return co, nil
}

// baseURL builds the scheme + host:port URL for endpoint. h2c if TLS is
// not configured, https otherwise.
func (c *Client) baseURL(endpoint string) string {
	if c.cfg.ClientTLSConfig != nil {
		return "https://" + endpoint
	}
	return "http://" + endpoint
}

// newTransport returns a fresh http.RoundTripper for one endpoint. Honors
// ClientConfig.Transport if set (tests), otherwise builds an
// *http.Transport with HTTP/2 or h2c selected by ClientTLSConfig.
func (c *Client) newTransport() (http.RoundTripper, error) {
	if c.cfg.Transport != nil {
		return c.cfg.Transport, nil
	}
	tr := &http.Transport{Protocols: new(http.Protocols)}
	if c.cfg.ClientTLSConfig != nil {
		tr.Protocols.SetHTTP2(true)
		tr.Protocols.SetHTTP1(false)
		tr.TLSClientConfig = c.cfg.ClientTLSConfig.Clone()
	} else {
		tr.Protocols.SetUnencryptedHTTP2(true)
		tr.Protocols.SetHTTP1(false)
	}
	return tr, nil
}

// Close releases all pooled connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, co := range c.conns {
		closeTransport(co.tr)
	}
	c.conns = nil
	return nil
}

// closeTransport drops idle connections on *http.Transport; no-op for
// other RoundTripper implementations (test fakes).
func closeTransport(tr http.RoundTripper) {
	if t, ok := tr.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
}
