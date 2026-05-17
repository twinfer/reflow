package admin

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/twinfer/reflow/proto/adminv1"
)

// fakeAdmin lets each test express AddNode behavior as a closure. All
// other RPCs are not used by these tests and return Unimplemented via
// the embedded UnimplementedAdminServer.
type fakeAdmin struct {
	adminv1.UnimplementedAdminServer
	addNode func(context.Context, *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error)
}

func (f *fakeAdmin) AddNode(ctx context.Context, req *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
	return f.addNode(ctx, req)
}

// startFakeAdmin spawns a gRPC server bound to a free loopback port and
// returns its host:port plus a stop func. Insecure transport — these
// tests exercise the redirect plumbing, not the auth path.
func startFakeAdmin(t *testing.T, behavior *fakeAdmin) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	adminv1.RegisterAdminServer(srv, behavior)
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), func() { srv.Stop() }
}

// unavailableWithHint returns codes.Unavailable carrying a LeaderHint
// pointing at hintAddr.
func unavailableWithHint(hintAddr string) error {
	st := status.New(codes.Unavailable, "not the metadata leader")
	if hintAddr == "" {
		return st.Err()
	}
	withDetails, err := st.WithDetails(&adminv1.LeaderHint{
		NodeId:        1,
		AdminEndpoint: hintAddr,
	})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

func TestCallWithLeaderRedirect_FirstHopSucceeds(t *testing.T) {
	var calls int32
	leader := &fakeAdmin{addNode: func(_ context.Context, _ *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
		atomic.AddInt32(&calls, 1)
		return &adminv1.AddNodeResponse{AssignmentEpoch: 42}, nil
	}}
	addr, stop := startFakeAdmin(t, leader)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := CallWithLeaderRedirect(ctx, DialOptions{Addr: addr}, 3,
		func(rctx context.Context, cli adminv1.AdminClient) error {
			resp, err := cli.AddNode(rctx, &adminv1.AddNodeRequest{NodeId: 4, RaftAddr: "x"})
			if err != nil {
				return err
			}
			if resp.GetAssignmentEpoch() != 42 {
				t.Fatalf("epoch: want 42, got %d", resp.GetAssignmentEpoch())
			}
			return nil
		})
	if err != nil {
		t.Fatalf("CallWithLeaderRedirect: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("leader.AddNode calls: want 1, got %d", got)
	}
}

func TestCallWithLeaderRedirect_FollowsHintToLeader(t *testing.T) {
	var leaderCalls int32
	leader := &fakeAdmin{addNode: func(_ context.Context, _ *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
		atomic.AddInt32(&leaderCalls, 1)
		return &adminv1.AddNodeResponse{AssignmentEpoch: 7}, nil
	}}
	leaderAddr, stopL := startFakeAdmin(t, leader)
	defer stopL()

	follower := &fakeAdmin{addNode: func(_ context.Context, _ *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
		return nil, unavailableWithHint(leaderAddr)
	}}
	followerAddr, stopF := startFakeAdmin(t, follower)
	defer stopF()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := CallWithLeaderRedirect(ctx, DialOptions{Addr: followerAddr}, 3,
		func(rctx context.Context, cli adminv1.AdminClient) error {
			_, err := cli.AddNode(rctx, &adminv1.AddNodeRequest{NodeId: 4, RaftAddr: "x"})
			return err
		})
	if err != nil {
		t.Fatalf("CallWithLeaderRedirect: %v", err)
	}
	if got := atomic.LoadInt32(&leaderCalls); got != 1 {
		t.Fatalf("leader.AddNode calls: want 1, got %d", got)
	}
}

func TestCallWithLeaderRedirect_LoopGuardOnSelfHint(t *testing.T) {
	// Server hints at its OWN address — naive follow would loop. Helper
	// must break out and return the original Unavailable.
	var addrHolder string
	srv := &fakeAdmin{}
	srv.addNode = func(_ context.Context, _ *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
		return nil, unavailableWithHint(addrHolder)
	}
	addr, stop := startFakeAdmin(t, srv)
	defer stop()
	addrHolder = addr

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := CallWithLeaderRedirect(ctx, DialOptions{Addr: addr}, 5,
		func(rctx context.Context, cli adminv1.AdminClient) error {
			_, err := cli.AddNode(rctx, &adminv1.AddNodeRequest{NodeId: 4, RaftAddr: "x"})
			return err
		})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable on self-loop, got %v", err)
	}
}

func TestCallWithLeaderRedirect_TerminalErrorShortCircuits(t *testing.T) {
	var calls int32
	srv := &fakeAdmin{addNode: func(_ context.Context, _ *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
		atomic.AddInt32(&calls, 1)
		return nil, status.Error(codes.PermissionDenied, "no")
	}}
	addr, stop := startFakeAdmin(t, srv)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := CallWithLeaderRedirect(ctx, DialOptions{Addr: addr}, 5,
		func(rctx context.Context, cli adminv1.AdminClient) error {
			_, err := cli.AddNode(rctx, &adminv1.AddNodeRequest{NodeId: 4, RaftAddr: "x"})
			return err
		})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("want PermissionDenied to propagate untouched, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("server calls: want 1 (terminal short-circuits), got %d", got)
	}
}

func TestCallWithLeaderRedirect_HopsExhausted(t *testing.T) {
	// Build a cycle A → B → A. Both return Unavailable + a hint pointing
	// at the other. With maxHops=3, we should exhaust without success.
	var aAddr, bAddr string
	a := &fakeAdmin{}
	b := &fakeAdmin{}
	a.addNode = func(_ context.Context, _ *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
		return nil, unavailableWithHint(bAddr)
	}
	b.addNode = func(_ context.Context, _ *adminv1.AddNodeRequest) (*adminv1.AddNodeResponse, error) {
		return nil, unavailableWithHint(aAddr)
	}
	addrA, stopA := startFakeAdmin(t, a)
	defer stopA()
	addrB, stopB := startFakeAdmin(t, b)
	defer stopB()
	aAddr, bAddr = addrA, addrB

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := CallWithLeaderRedirect(ctx, DialOptions{Addr: addrA}, 3,
		func(rctx context.Context, cli adminv1.AdminClient) error {
			_, err := cli.AddNode(rctx, &adminv1.AddNodeRequest{NodeId: 4, RaftAddr: "x"})
			return err
		})
	if err == nil {
		t.Fatal("want error on exhausted hops, got nil")
	}
	if !strings.Contains(err.Error(), "exhausted") {
		t.Fatalf("want error message to mention exhausted, got %v", err)
	}
}
