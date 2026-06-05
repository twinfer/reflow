package reflowclient

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	clusterctlv1 "github.com/twinfer/reflw/proto/clusterctlv1"
	"github.com/twinfer/reflw/proto/clusterctlv1/clusterctlv1connect"
	configv1 "github.com/twinfer/reflw/proto/configv1"
	"github.com/twinfer/reflw/proto/configv1/configv1connect"
)

// fakeCluster lets each test express AddNode behavior as a closure.
// All other RPCs return Unimplemented via the embedded handler.
type fakeCluster struct {
	clusterctlv1connect.UnimplementedClusterCtlHandler
	addNode func(context.Context, *clusterctlv1.AddNodeRequest) (*clusterctlv1.AddNodeResponse, error)
}

func (f *fakeCluster) AddNode(ctx context.Context, req *connect.Request[clusterctlv1.AddNodeRequest]) (*connect.Response[clusterctlv1.AddNodeResponse], error) {
	resp, err := f.addNode(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

// startFakeCluster spawns a Connect/h2c server on a free loopback port
// hosting only the ClusterCtl handler. Insecure transport — these
// tests exercise the redirect plumbing, not auth.
func startFakeCluster(t *testing.T, behavior *fakeCluster) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := http.NewServeMux()
	path, h := clusterctlv1connect.NewClusterCtlHandler(behavior)
	mux.Handle(path, h)
	srv := &http.Server{Handler: mux, Protocols: new(http.Protocols)}
	srv.Protocols.SetUnencryptedHTTP2(true)
	srv.Protocols.SetHTTP1(false)
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		_ = ln.Close()
	}
}

// unavailableWithClusterHint returns CodeUnavailable carrying a
// clusterctlv1.LeaderHint pointing at hintAddr.
func unavailableWithClusterHint(hintAddr string) error {
	cerr := connect.NewError(connect.CodeUnavailable, errors.New("not the metadata leader"))
	if hintAddr == "" {
		return cerr
	}
	if d, err := connect.NewErrorDetail(&clusterctlv1.LeaderHint{
		NodeId:        1,
		AdminEndpoint: hintAddr,
	}); err == nil {
		cerr.AddDetail(d)
	}
	return cerr
}

// unavailableWithConfigHint returns CodeUnavailable carrying a
// configv1.LeaderHint pointing at hintAddr. Used to assert the
// redirect helper handles either detail type.
func unavailableWithConfigHint(hintAddr string) error {
	cerr := connect.NewError(connect.CodeUnavailable, errors.New("not the metadata leader"))
	if hintAddr == "" {
		return cerr
	}
	if d, err := connect.NewErrorDetail(&configv1.LeaderHint{
		NodeId:        1,
		AdminEndpoint: hintAddr,
	}); err == nil {
		cerr.AddDetail(d)
	}
	return cerr
}

func TestCallWithLeaderRedirect_FirstHopSucceeds(t *testing.T) {
	var calls int32
	leader := &fakeCluster{addNode: func(_ context.Context, _ *clusterctlv1.AddNodeRequest) (*clusterctlv1.AddNodeResponse, error) {
		atomic.AddInt32(&calls, 1)
		return &clusterctlv1.AddNodeResponse{AssignmentEpoch: 42}, nil
	}}
	addr, stop := startFakeCluster(t, leader)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := CallWithLeaderRedirect(ctx, DialOptions{Addr: addr}, 3,
		func(rctx context.Context, cli *Client) error {
			resp, err := cli.Cluster.AddNode(rctx, connect.NewRequest(&clusterctlv1.AddNodeRequest{NodeId: 4, RaftAddr: "x"}))
			if err != nil {
				return err
			}
			if resp.Msg.GetAssignmentEpoch() != 42 {
				t.Fatalf("epoch: want 42, got %d", resp.Msg.GetAssignmentEpoch())
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
	leader := &fakeCluster{addNode: func(_ context.Context, _ *clusterctlv1.AddNodeRequest) (*clusterctlv1.AddNodeResponse, error) {
		atomic.AddInt32(&leaderCalls, 1)
		return &clusterctlv1.AddNodeResponse{AssignmentEpoch: 7}, nil
	}}
	leaderAddr, stopL := startFakeCluster(t, leader)
	defer stopL()

	follower := &fakeCluster{addNode: func(_ context.Context, _ *clusterctlv1.AddNodeRequest) (*clusterctlv1.AddNodeResponse, error) {
		return nil, unavailableWithClusterHint(leaderAddr)
	}}
	followerAddr, stopF := startFakeCluster(t, follower)
	defer stopF()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := CallWithLeaderRedirect(ctx, DialOptions{Addr: followerAddr}, 3,
		func(rctx context.Context, cli *Client) error {
			_, err := cli.Cluster.AddNode(rctx, connect.NewRequest(&clusterctlv1.AddNodeRequest{NodeId: 4, RaftAddr: "x"}))
			return err
		})
	if err != nil {
		t.Fatalf("CallWithLeaderRedirect: %v", err)
	}
	if got := atomic.LoadInt32(&leaderCalls); got != 1 {
		t.Fatalf("leader.AddNode calls: want 1, got %d", got)
	}
}

// TestCallWithLeaderRedirect_FollowsConfigHint covers the cross-service
// path: a Config-side error carries configv1.LeaderHint, and the
// redirect helper must still chase it (it walks the connect.Error
// details for either flavor).
func TestCallWithLeaderRedirect_FollowsConfigHint(t *testing.T) {
	var leaderCalls int32
	leader := &fakeCluster{addNode: func(_ context.Context, _ *clusterctlv1.AddNodeRequest) (*clusterctlv1.AddNodeResponse, error) {
		atomic.AddInt32(&leaderCalls, 1)
		return &clusterctlv1.AddNodeResponse{AssignmentEpoch: 7}, nil
	}}
	leaderAddr, stopL := startFakeCluster(t, leader)
	defer stopL()

	follower := &fakeCluster{addNode: func(_ context.Context, _ *clusterctlv1.AddNodeRequest) (*clusterctlv1.AddNodeResponse, error) {
		return nil, unavailableWithConfigHint(leaderAddr)
	}}
	followerAddr, stopF := startFakeCluster(t, follower)
	defer stopF()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := CallWithLeaderRedirect(ctx, DialOptions{Addr: followerAddr}, 3,
		func(rctx context.Context, cli *Client) error {
			_, err := cli.Cluster.AddNode(rctx, connect.NewRequest(&clusterctlv1.AddNodeRequest{NodeId: 4, RaftAddr: "x"}))
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
	srv := &fakeCluster{}
	srv.addNode = func(_ context.Context, _ *clusterctlv1.AddNodeRequest) (*clusterctlv1.AddNodeResponse, error) {
		return nil, unavailableWithClusterHint(addrHolder)
	}
	addr, stop := startFakeCluster(t, srv)
	defer stop()
	addrHolder = addr

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := CallWithLeaderRedirect(ctx, DialOptions{Addr: addr}, 5,
		func(rctx context.Context, cli *Client) error {
			_, err := cli.Cluster.AddNode(rctx, connect.NewRequest(&clusterctlv1.AddNodeRequest{NodeId: 4, RaftAddr: "x"}))
			return err
		})
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("want Unavailable on self-loop, got %v", err)
	}
}

func TestCallWithLeaderRedirect_TerminalErrorShortCircuits(t *testing.T) {
	var calls int32
	srv := &fakeCluster{addNode: func(_ context.Context, _ *clusterctlv1.AddNodeRequest) (*clusterctlv1.AddNodeResponse, error) {
		atomic.AddInt32(&calls, 1)
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("no"))
	}}
	addr, stop := startFakeCluster(t, srv)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := CallWithLeaderRedirect(ctx, DialOptions{Addr: addr}, 5,
		func(rctx context.Context, cli *Client) error {
			_, err := cli.Cluster.AddNode(rctx, connect.NewRequest(&clusterctlv1.AddNodeRequest{NodeId: 4, RaftAddr: "x"}))
			return err
		})
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("want PermissionDenied to propagate untouched, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("server calls: want 1 (terminal short-circuits), got %d", got)
	}
}

func TestCallWithLeaderRedirect_HopsExhausted(t *testing.T) {
	// Build a cycle A → B → A. Both return Unavailable + hint to the
	// other. With maxHops=3, exhaust without success.
	var aAddr, bAddr string
	a := &fakeCluster{}
	b := &fakeCluster{}
	a.addNode = func(_ context.Context, _ *clusterctlv1.AddNodeRequest) (*clusterctlv1.AddNodeResponse, error) {
		return nil, unavailableWithClusterHint(bAddr)
	}
	b.addNode = func(_ context.Context, _ *clusterctlv1.AddNodeRequest) (*clusterctlv1.AddNodeResponse, error) {
		return nil, unavailableWithClusterHint(aAddr)
	}
	addrA, stopA := startFakeCluster(t, a)
	defer stopA()
	addrB, stopB := startFakeCluster(t, b)
	defer stopB()
	aAddr, bAddr = addrA, addrB

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := CallWithLeaderRedirect(ctx, DialOptions{Addr: addrA}, 3,
		func(rctx context.Context, cli *Client) error {
			_, err := cli.Cluster.AddNode(rctx, connect.NewRequest(&clusterctlv1.AddNodeRequest{NodeId: 4, RaftAddr: "x"}))
			return err
		})
	if err == nil {
		t.Fatal("want error on exhausted hops, got nil")
	}
	if !strings.Contains(err.Error(), "exhausted") {
		t.Fatalf("want error message to mention exhausted, got %v", err)
	}
}

// _ = configv1connect placeholder kept so the import is referenced
// even if the only configv1 test detail is the hint type.
var _ = configv1connect.NewConfigClient
