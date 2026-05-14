package loadgen

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/dragonboat/v4/raftpb"
	"google.golang.org/grpc/test/bufconn"
)

// PartitionMatrix gates which unordered pairs of RaftAddresses are
// allowed to exchange raft traffic. The chaos harness mutates the
// matrix mid-run; the bufconn transport re-checks it on every send so
// Cut tears in-flight links and Heal restores them without dialing
// gymnastics. The default (zero-value) matrix allows every pair —
// callers Cut to inject a partition.
//
// Used together with BufconnHub and BufconnTransportFactory; see
// NewBufconnTransportFactory.
type PartitionMatrix struct {
	mu      sync.RWMutex
	dropped map[[2]string]bool
}

// NewPartitionMatrix returns a matrix that allows every pair.
func NewPartitionMatrix() *PartitionMatrix {
	return &PartitionMatrix{dropped: make(map[[2]string]bool)}
}

// Cut marks the unordered pair (a, b) as partitioned. Subsequent
// SendMessageBatch / SendChunk calls in either direction return
// errPartitioned until Heal is called for the same pair.
func (m *PartitionMatrix) Cut(a, b string) {
	if a == b {
		return
	}
	k := pairKey(a, b)
	m.mu.Lock()
	m.dropped[k] = true
	m.mu.Unlock()
}

// Heal removes a previously-Cut partition between (a, b). No-op if
// the pair was not partitioned.
func (m *PartitionMatrix) Heal(a, b string) {
	if a == b {
		return
	}
	k := pairKey(a, b)
	m.mu.Lock()
	delete(m.dropped, k)
	m.mu.Unlock()
}

// Allowed reports whether traffic from -> to is permitted. The check
// is symmetric — Cut(a, b) blocks both directions.
func (m *PartitionMatrix) Allowed(from, to string) bool {
	if from == to {
		return true
	}
	k := pairKey(from, to)
	m.mu.RLock()
	defer m.mu.RUnlock()
	return !m.dropped[k]
}

func pairKey(a, b string) [2]string {
	if a < b {
		return [2]string{a, b}
	}
	return [2]string{b, a}
}

// BufconnHub maps a node's RaftAddress to its bufconn.Listener. The
// chaos harness creates one Hub per cluster and shares it across
// every BufconnTransportFactory.Create call so dialers can find
// their target.
type BufconnHub struct {
	mu        sync.Mutex
	listeners map[string]*bufconn.Listener
}

// NewBufconnHub returns an empty hub.
func NewBufconnHub() *BufconnHub {
	return &BufconnHub{listeners: make(map[string]*bufconn.Listener)}
}

// register associates a bufconn listener with a RaftAddress. The
// listener is reused across reconnects: dragonboat dials and re-dials
// freely on transport errors, so the listener outlives any single
// IConnection. unregister removes the entry on transport Close so a
// rebooted node can re-register with a fresh listener.
func (h *BufconnHub) register(raftAddr string, lis *bufconn.Listener) {
	h.mu.Lock()
	h.listeners[raftAddr] = lis
	h.mu.Unlock()
}

func (h *BufconnHub) unregister(raftAddr string) {
	h.mu.Lock()
	delete(h.listeners, raftAddr)
	h.mu.Unlock()
}

// dial returns a net.Conn dialed against the target's bufconn
// listener, or errPeerDown if no listener is registered.
func (h *BufconnHub) dial(ctx context.Context, target string) (net.Conn, error) {
	h.mu.Lock()
	lis, ok := h.listeners[target]
	h.mu.Unlock()
	if !ok {
		return nil, errPeerDown
	}
	return lis.DialContext(ctx)
}

// errPartitioned is returned for sends that the matrix forbids. It is
// a plain error so dragonboat's transport layer treats it as a
// transient failure and retries (matching the behavior of a dropped
// packet on a real partitioned network).
var errPartitioned = errors.New("bufconn: pair partitioned")

// errPeerDown is returned when the dial target has no listener
// registered in the hub (the peer's transport has not started yet,
// or the node has been killed). Distinct from errPartitioned so
// chaos test triage can tell "killed" from "partitioned" apart.
var errPeerDown = errors.New("bufconn: peer down")

// BufconnTransportFactory implements config.TransportFactory by
// returning bufconn-backed raftio.ITransport instances. All
// transports share the same hub (so they can dial each other) and
// the same matrix (so a single Cut applies cluster-wide).
type BufconnTransportFactory struct {
	hub    *BufconnHub
	matrix *PartitionMatrix
	bufSz  int
}

// NewBufconnTransportFactory builds a factory wired to the given hub
// + matrix. bufSize is the bufconn listener buffer; 1 << 20 is a
// reasonable default for raft message volumes.
func NewBufconnTransportFactory(hub *BufconnHub, matrix *PartitionMatrix) *BufconnTransportFactory {
	return &BufconnTransportFactory{hub: hub, matrix: matrix, bufSz: 1 << 20}
}

// Create satisfies config.TransportFactory.
func (f *BufconnTransportFactory) Create(nhCfg config.NodeHostConfig, msgH raftio.MessageHandler, chunkH raftio.ChunkHandler) raftio.ITransport {
	return &bufconnTransport{
		self:    nhCfg.RaftAddress,
		hub:     f.hub,
		matrix:  f.matrix,
		bufSz:   f.bufSz,
		msgH:    msgH,
		chunkH:  chunkH,
		closeCh: make(chan struct{}),
	}
}

// Validate accepts any non-empty address — RaftAddress is opaque
// when a custom transport is in use.
func (f *BufconnTransportFactory) Validate(addr string) bool {
	return addr != ""
}

// bufconnTransport implements raftio.ITransport over a per-node
// bufconn.Listener registered in a shared BufconnHub.
type bufconnTransport struct {
	self    string
	hub     *BufconnHub
	matrix  *PartitionMatrix
	bufSz   int
	msgH    raftio.MessageHandler
	chunkH  raftio.ChunkHandler
	closeCh chan struct{}

	mu       sync.Mutex
	listener *bufconn.Listener
	// accepted tracks server-side connections returned from Accept so
	// Close can close them and unblock readLoop's io.ReadFull. bufconn
	// listener close does not propagate to accepted pipes; without
	// this set Close would deadlock waiting for readers that block on
	// connections still considered open.
	accepted map[net.Conn]struct{}
	closed   bool
	wg       sync.WaitGroup
}

func (t *bufconnTransport) Name() string { return "bufconn" }

// Start registers a fresh bufconn listener for t.self and runs the
// accept loop. Each accepted connection spawns a reader goroutine
// that decodes length-prefixed frames and dispatches to the message
// or chunk handler based on the frame tag.
func (t *bufconnTransport) Start() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return errors.New("bufconn: Start on closed transport")
	}
	lis := bufconn.Listen(t.bufSz)
	t.listener = lis
	t.accepted = make(map[net.Conn]struct{})
	t.mu.Unlock()
	t.hub.register(t.self, lis)
	t.wg.Add(1)
	go t.acceptLoop(lis)
	return nil
}

// Close tears the listener down, closes every accepted server-side
// connection (unblocking readLoop's io.ReadFull), unregisters from
// the hub, and waits for the accept loop + reader goroutines to exit.
func (t *bufconnTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.closeCh)
	lis := t.listener
	t.listener = nil
	accepted := t.accepted
	t.accepted = nil
	t.mu.Unlock()
	t.hub.unregister(t.self)
	if lis != nil {
		_ = lis.Close()
	}
	for c := range accepted {
		_ = c.Close()
	}
	t.wg.Wait()
	return nil
}

func (t *bufconnTransport) acceptLoop(lis *bufconn.Listener) {
	defer t.wg.Done()
	for {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			_ = conn.Close()
			return
		}
		t.accepted[conn] = struct{}{}
		t.mu.Unlock()
		t.wg.Add(1)
		go t.readLoop(conn)
	}
}

// readLoop reads framed proto payloads until EOF or error. Each
// frame: [1 byte tag][4 byte big-endian length][marshaled proto].
// Tag 0 = raftpb.MessageBatch -> msgH. Tag 1 = raftpb.Chunk -> chunkH;
// if chunkH returns false the connection is closed (matches
// dragonboat semantics: invalid for future chunks).
func (t *bufconnTransport) readLoop(conn net.Conn) {
	defer t.wg.Done()
	defer func() {
		_ = conn.Close()
		t.mu.Lock()
		if t.accepted != nil {
			delete(t.accepted, conn)
		}
		t.mu.Unlock()
	}()
	hdr := make([]byte, 5)
	for {
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return
		}
		tag := hdr[0]
		n := binary.BigEndian.Uint32(hdr[1:])
		buf := make([]byte, n)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}
		switch tag {
		case frameTagMessageBatch:
			var batch raftpb.MessageBatch
			if err := batch.Unmarshal(buf); err != nil {
				return
			}
			if t.msgH != nil {
				t.msgH(batch)
			}
		case frameTagChunk:
			var chunk raftpb.Chunk
			if err := chunk.Unmarshal(buf); err != nil {
				return
			}
			if t.chunkH != nil && !t.chunkH(chunk) {
				return
			}
		default:
			return
		}
	}
}

// GetConnection returns a bufconnConn dialed against the target's
// listener. The matrix is checked at dial time AND on every send so
// Cut/Heal applied after the dial take effect immediately.
func (t *bufconnTransport) GetConnection(ctx context.Context, target string) (raftio.IConnection, error) {
	if !t.matrix.Allowed(t.self, target) {
		return nil, fmt.Errorf("%w: %s -> %s", errPartitioned, t.self, target)
	}
	c, err := t.hub.dial(ctx, target)
	if err != nil {
		return nil, err
	}
	return &bufconnConn{self: t.self, target: target, conn: c, matrix: t.matrix}, nil
}

// GetSnapshotConnection mirrors GetConnection for snapshot chunks.
func (t *bufconnTransport) GetSnapshotConnection(ctx context.Context, target string) (raftio.ISnapshotConnection, error) {
	if !t.matrix.Allowed(t.self, target) {
		return nil, fmt.Errorf("%w: %s -> %s", errPartitioned, t.self, target)
	}
	c, err := t.hub.dial(ctx, target)
	if err != nil {
		return nil, err
	}
	return &bufconnSnapConn{self: t.self, target: target, conn: c, matrix: t.matrix}, nil
}

const (
	frameTagMessageBatch byte = 0
	frameTagChunk        byte = 1
)

// bufconnConn implements raftio.IConnection.
type bufconnConn struct {
	self   string
	target string
	conn   net.Conn
	matrix *PartitionMatrix
	wmu    sync.Mutex
}

func (c *bufconnConn) Close() {
	_ = c.conn.Close()
}

func (c *bufconnConn) SendMessageBatch(batch raftpb.MessageBatch) error {
	if !c.matrix.Allowed(c.self, c.target) {
		return fmt.Errorf("%w: %s -> %s", errPartitioned, c.self, c.target)
	}
	buf, err := batch.Marshal()
	if err != nil {
		return fmt.Errorf("bufconn: marshal MessageBatch: %w", err)
	}
	return writeFrame(&c.wmu, c.conn, frameTagMessageBatch, buf)
}

// bufconnSnapConn implements raftio.ISnapshotConnection.
type bufconnSnapConn struct {
	self   string
	target string
	conn   net.Conn
	matrix *PartitionMatrix
	wmu    sync.Mutex
}

func (c *bufconnSnapConn) Close() {
	_ = c.conn.Close()
}

func (c *bufconnSnapConn) SendChunk(chunk raftpb.Chunk) error {
	if !c.matrix.Allowed(c.self, c.target) {
		return fmt.Errorf("%w: %s -> %s", errPartitioned, c.self, c.target)
	}
	buf, err := chunk.Marshal()
	if err != nil {
		return fmt.Errorf("bufconn: marshal Chunk: %w", err)
	}
	return writeFrame(&c.wmu, c.conn, frameTagChunk, buf)
}

// writeFrame serializes [tag][len][payload] under the connection's
// write mutex. bufconn pipes are happy with concurrent writes, but
// frame boundaries must be atomic so the reader can decode.
func writeFrame(mu *sync.Mutex, w io.Writer, tag byte, payload []byte) error {
	if len(payload) > 1<<31-1 {
		return fmt.Errorf("bufconn: frame too large: %d bytes", len(payload))
	}
	hdr := [5]byte{tag}
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	mu.Lock()
	defer mu.Unlock()
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return nil
}

// ensure interface satisfaction at compile time.
var (
	_ config.TransportFactory    = (*BufconnTransportFactory)(nil)
	_ raftio.ITransport          = (*bufconnTransport)(nil)
	_ raftio.IConnection         = (*bufconnConn)(nil)
	_ raftio.ISnapshotConnection = (*bufconnSnapConn)(nil)
)
