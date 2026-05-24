package delivery

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/auth"
	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
	"github.com/twinfer/reflow/proto/deliveryv1/deliveryv1connect"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// stubResolver maps a shard to (nodeID, endpoint) — endpoint is the
// host:port of the in-process httptest server.
type stubResolver struct {
	leader   map[uint64]uint64
	endpoint map[uint64]string
}

func (r *stubResolver) PartitionLeaderHint(shardID uint64) (uint64, bool) {
	id, ok := r.leader[shardID]
	return id, ok
}
func (r *stubResolver) NodeEndpoint(nodeID uint64) (string, bool) {
	ep, ok := r.endpoint[nodeID]
	return ep, ok
}

// stubHandler implements deliveryv1connect.DeliveryHandler. respond is
// invoked per DeliverRequest and produces the reply.
type stubHandler struct {
	deliveryv1connect.UnimplementedDeliveryHandler
	respond func(*deliveryv1.DeliverRequest) *deliveryv1.DeliverResponse

	mu       sync.Mutex
	received []*deliveryv1.DeliverRequest
}

func (s *stubHandler) Deliver(ctx context.Context, stream *connect.BidiStream[deliveryv1.DeliverRequest, deliveryv1.DeliverResponse]) error {
	for {
		req, err := stream.Receive()
		if err != nil {
			// EOF on the request half ends the loop cleanly.
			return nil
		}
		s.mu.Lock()
		s.received = append(s.received, req)
		s.mu.Unlock()
		if err := stream.Send(s.respond(req)); err != nil {
			return err
		}
	}
}

// writeTestPolicy writes a permissive policy that allows anonymous
// access to /reflow.delivery.v1.Delivery/*. Wiring tests still go
// through auth.HTTPMiddleware so the middleware/policy/extractor stack
// runs end-to-end; TLS + node-cert fixtures are exercised separately by
// the integration tests in internal/engine.
func writeTestPolicy(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "policy.json")
	body := `{
  "allow_rules": [
    {
      "name": "test_delivery_open",
      "request": {"paths": ["/reflow.delivery.v1.Delivery/*"]}
    }
  ]
}`
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write policy: %v", err)
	}
	return p
}

// startTestDelivery stands up the Connect Delivery handler over h2c on a
// random local port. The handler is wrapped by auth.HTTPMiddleware using
// the temp test policy so the auth flow exercises in tests.
func startTestDelivery(t *testing.T, h *stubHandler) (*Client, func()) {
	t.Helper()

	mw, mwCloser, _, err := auth.HTTPMiddleware(auth.Config{PolicyFile: writeTestPolicy(t)}, nil)
	if err != nil {
		t.Fatalf("HTTPMiddleware: %v", err)
	}

	path, handler := deliveryv1connect.NewDeliveryHandler(h)
	mux := http.NewServeMux()
	mux.Handle(path, mw(handler))

	srv := &http.Server{Handler: mux, Protocols: new(http.Protocols)}
	srv.Protocols.SetUnencryptedHTTP2(true)
	srv.Protocols.SetHTTP1(false)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()

	cli, err := NewClient(ClientConfig{
		Resolver: &stubResolver{
			leader:   map[uint64]uint64{7: 1},
			endpoint: map[uint64]string{1: ln.Addr().String()},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	cleanup := func() {
		_ = cli.Close()
		_ = srv.Close()
		_ = ln.Close()
		if mwCloser != nil {
			_ = mwCloser()
		}
	}
	return cli, cleanup
}

func TestDeliveryClient_Ack(t *testing.T) {
	h := &stubHandler{
		respond: func(req *deliveryv1.DeliverRequest) *deliveryv1.DeliverResponse {
			return &deliveryv1.DeliverResponse{
				Seq:  req.GetSeq(),
				Kind: &deliveryv1.DeliverResponse_Ack{Ack: &deliveryv1.Ack{}},
			}
		},
	}
	cli, cleanup := startTestDelivery(t, h)
	defer cleanup()

	if err := cli.Send(context.Background(), 7, "outbox/p1", 42, &enginev1.Command{}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.received) != 1 || h.received[0].GetSeq() != 42 || h.received[0].GetShardId() != 7 {
		t.Fatalf("server got unexpected requests: %+v", h.received)
	}
}

func TestDeliveryClient_NotLeader(t *testing.T) {
	h := &stubHandler{
		respond: func(req *deliveryv1.DeliverRequest) *deliveryv1.DeliverResponse {
			return &deliveryv1.DeliverResponse{
				Seq: req.GetSeq(),
				Kind: &deliveryv1.DeliverResponse_NotLeader{
					NotLeader: &deliveryv1.NotLeader{LeaderNodeId: 99},
				},
			}
		},
	}
	cli, cleanup := startTestDelivery(t, h)
	defer cleanup()

	err := cli.Send(context.Background(), 7, "outbox/p1", 1, &enginev1.Command{})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader; got %v", err)
	}
}

func TestDeliveryClient_Err(t *testing.T) {
	h := &stubHandler{
		respond: func(req *deliveryv1.DeliverRequest) *deliveryv1.DeliverResponse {
			return &deliveryv1.DeliverResponse{
				Seq: req.GetSeq(),
				Kind: &deliveryv1.DeliverResponse_Err{
					Err: &deliveryv1.Err{Message: "boom"},
				},
			}
		},
	}
	cli, cleanup := startTestDelivery(t, h)
	defer cleanup()

	err := cli.Send(context.Background(), 7, "outbox/p1", 1, &enginev1.Command{})
	if err == nil || !contains(err.Error(), "boom") {
		t.Fatalf("expected err containing 'boom'; got %v", err)
	}
}

func TestDeliveryClient_NoLeaderHint(t *testing.T) {
	cli, err := NewClient(ClientConfig{
		Resolver: &stubResolver{
			leader:   map[uint64]uint64{}, // empty
			endpoint: map[uint64]string{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	err = cli.Send(context.Background(), 7, "outbox/p1", 1, &enginev1.Command{})
	if err == nil || !contains(err.Error(), "no leader known") {
		t.Fatalf("expected 'no leader known' error; got %v", err)
	}
}

// TestDeliveryClient_PolicyDenies stands up the handler with a strict
// policy that requires a node/* principal. The h2c client dials without
// a peer cert (anonymous), so the policy must reject. Anonymous denials
// map to HTTP 401 / connect.CodeUnauthenticated (the auth middleware
// splits anonymous vs authenticated denials at internal/auth/connect.go);
// the client surfaces it as a non-Ack error. Exercises the auth path
// from the inside without TLS fixtures.
func TestDeliveryClient_PolicyDenies(t *testing.T) {
	dir := t.TempDir()
	policy := filepath.Join(dir, "policy.json")
	body := `{
  "allow_rules": [
    {
      "name": "deny_anonymous_delivery",
      "request": {
        "paths": ["/reflow.delivery.v1.Delivery/*"],
        "headers": [{"key": "x-reflow-principal", "values": ["node/*"]}]
      }
    }
  ]
}`
	if err := os.WriteFile(policy, []byte(body), 0o600); err != nil {
		t.Fatalf("write policy: %v", err)
	}

	mw, mwCloser, _, err := auth.HTTPMiddleware(auth.Config{PolicyFile: policy}, nil)
	if err != nil {
		t.Fatalf("HTTPMiddleware: %v", err)
	}
	defer func() {
		if mwCloser != nil {
			_ = mwCloser()
		}
	}()

	h := &stubHandler{respond: func(req *deliveryv1.DeliverRequest) *deliveryv1.DeliverResponse {
		t.Fatalf("handler reached despite anonymous caller; received %+v", req)
		return nil
	}}
	path, handler := deliveryv1connect.NewDeliveryHandler(h)
	mux := http.NewServeMux()
	mux.Handle(path, mw(handler))

	srv := &http.Server{Handler: mux, Protocols: new(http.Protocols)}
	srv.Protocols.SetUnencryptedHTTP2(true)
	srv.Protocols.SetHTTP1(false)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		_ = srv.Close()
		_ = ln.Close()
	}()

	cli, err := NewClient(ClientConfig{
		Resolver: &stubResolver{
			leader:   map[uint64]uint64{7: 1},
			endpoint: map[uint64]string{1: ln.Addr().String()},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cli.Close()

	sendErr := cli.Send(context.Background(), 7, "outbox/p1", 1, &enginev1.Command{})
	if sendErr == nil {
		t.Fatal("expected anonymous Send to be denied; got nil error")
	}
	if !contains(sendErr.Error(), "unauthorized") && connect.CodeOf(sendErr) != connect.CodeUnauthenticated {
		t.Fatalf("expected unauthorized / Unauthenticated; got %v", sendErr)
	}
}

func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
