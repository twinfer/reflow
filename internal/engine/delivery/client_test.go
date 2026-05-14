package delivery

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	deliveryv1 "github.com/twinfer/reflow/proto/deliveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// stubResolver maps a shard to (nodeID, endpoint) — endpoint is the
// bufconn name used by the dial hook below.
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

// stubServer is a bare-bones DeliveryServer for tests. respond is the
// closure that builds the reply for each request.
type stubServer struct {
	deliveryv1.UnimplementedDeliveryServer
	respond func(*deliveryv1.DeliverRequest) *deliveryv1.DeliverResponse

	mu       sync.Mutex
	received []*deliveryv1.DeliverRequest
}

func (s *stubServer) Deliver(stream deliveryv1.Delivery_DeliverServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.received = append(s.received, req)
		s.mu.Unlock()
		if err := stream.Send(s.respond(req)); err != nil {
			return err
		}
	}
}

// startBufconnDelivery returns a Client wired through bufconn to the
// given stub server. The bufconn endpoint name "bufnet" is registered as
// node 1's endpoint via the resolver.
func startBufconnDelivery(t *testing.T, srv *stubServer) (*Client, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	deliveryv1.RegisterDeliveryServer(grpcSrv, srv)
	go func() {
		_ = grpcSrv.Serve(lis)
	}()
	cli, err := NewClient(ClientConfig{
		Resolver: &stubResolver{
			leader:   map[uint64]uint64{7: 1},
			endpoint: map[uint64]string{1: "bufnet"},
		},
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
				return lis.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		_ = cli.Close()
		grpcSrv.Stop()
		_ = lis.Close()
	}
	return cli, cleanup
}

func TestDeliveryClient_Ack(t *testing.T) {
	srv := &stubServer{
		respond: func(req *deliveryv1.DeliverRequest) *deliveryv1.DeliverResponse {
			return &deliveryv1.DeliverResponse{
				Seq:  req.GetSeq(),
				Kind: &deliveryv1.DeliverResponse_Ack{Ack: &deliveryv1.Ack{}},
			}
		},
	}
	cli, cleanup := startBufconnDelivery(t, srv)
	defer cleanup()

	err := cli.Send(context.Background(), 7, "outbox/p1", 42, &enginev1.Command{})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(srv.received) != 1 || srv.received[0].GetSeq() != 42 || srv.received[0].GetShardId() != 7 {
		t.Fatalf("server got unexpected requests: %+v", srv.received)
	}
}

func TestDeliveryClient_NotLeader(t *testing.T) {
	srv := &stubServer{
		respond: func(req *deliveryv1.DeliverRequest) *deliveryv1.DeliverResponse {
			return &deliveryv1.DeliverResponse{
				Seq: req.GetSeq(),
				Kind: &deliveryv1.DeliverResponse_NotLeader{
					NotLeader: &deliveryv1.NotLeader{LeaderNodeId: 99},
				},
			}
		},
	}
	cli, cleanup := startBufconnDelivery(t, srv)
	defer cleanup()

	err := cli.Send(context.Background(), 7, "outbox/p1", 1, &enginev1.Command{})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader; got %v", err)
	}
}

func TestDeliveryClient_Err(t *testing.T) {
	srv := &stubServer{
		respond: func(req *deliveryv1.DeliverRequest) *deliveryv1.DeliverResponse {
			return &deliveryv1.DeliverResponse{
				Seq: req.GetSeq(),
				Kind: &deliveryv1.DeliverResponse_Err{
					Err: &deliveryv1.Err{Message: "boom"},
				},
			}
		},
	}
	cli, cleanup := startBufconnDelivery(t, srv)
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
