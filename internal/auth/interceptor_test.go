package auth

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	adminv1 "github.com/twinfer/reflow/proto/adminv1"
)

// stubExtractor returns canned (principal, err) regardless of input.
type stubExtractor struct {
	p   Principal
	err error
}

func (s stubExtractor) Extract(_ context.Context) (Principal, error) { return s.p, s.err }

// TestShim_StampsServerComputedPrincipal asserts the shim places
// Principal.Raw into incoming metadata under x-reflow-principal so a
// downstream interceptor sees the server-computed value.
func TestShim_StampsServerComputedPrincipal(t *testing.T) {
	unary, _ := newPrincipalShim(stubExtractor{p: Principal{Kind: "node", Subject: "7", Raw: "node/7"}}, nil)

	var seen string
	h := func(ctx context.Context, _ any) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if vals := md.Get(PrincipalHeader); len(vals) > 0 {
			seen = vals[0]
		}
		return "ok", nil
	}
	_, err := unary(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/reflow.delivery.v1.Delivery/Deliver"}, h)
	if err != nil {
		t.Fatal(err)
	}
	if seen != "node/7" {
		t.Errorf("downstream saw %q; want node/7", seen)
	}
}

// TestShim_StripsForgedPrincipalHeader is the load-bearing forgery
// test: a client sets x-reflow-principal=operator/admin in the outgoing
// metadata, but the shim must overwrite it with the server-extracted
// value (here: node/3) before any policy or handler sees it.
func TestShim_StripsForgedPrincipalHeader(t *testing.T) {
	unary, _ := newPrincipalShim(stubExtractor{p: Principal{Kind: "node", Subject: "3", Raw: "node/3"}}, nil)

	forged := metadata.Pairs(PrincipalHeader, "operator/admin")
	ctx := metadata.NewIncomingContext(context.Background(), forged)

	var seen []string
	h := func(ctx context.Context, _ any) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		seen = md.Get(PrincipalHeader)
		return "ok", nil
	}
	_, err := unary(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/reflow.delivery.v1.Delivery/Deliver"}, h)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 {
		t.Fatalf("expected exactly one %s header; got %v", PrincipalHeader, seen)
	}
	if seen[0] != "node/3" {
		t.Errorf("downstream saw %q; want node/3 (forged value must be overwritten)", seen[0])
	}
}

// TestShim_AnonymousLeavesHeaderUnset confirms that an anonymous
// principal does NOT stamp a header — the authz policy correctly
// denies based on missing identity rather than a default placeholder.
func TestShim_AnonymousLeavesHeaderUnset(t *testing.T) {
	unary, _ := newPrincipalShim(stubExtractor{p: Principal{}}, nil)

	forged := metadata.Pairs(PrincipalHeader, "operator/admin")
	ctx := metadata.NewIncomingContext(context.Background(), forged)

	var seen []string
	h := func(ctx context.Context, _ any) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		seen = md.Get(PrincipalHeader)
		return nil, nil
	}
	_, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/anything"}, h)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 0 {
		t.Errorf("expected anonymous → no header; got %v", seen)
	}
}

// TestShim_RejectsOnExtractorError surfaces the extractor error as
// Unauthenticated and never invokes the handler.
func TestShim_RejectsOnExtractorError(t *testing.T) {
	unary, _ := newPrincipalShim(stubExtractor{err: errors.New("bad cert")}, nil)
	var called bool
	h := func(_ context.Context, _ any) (any, error) { called = true; return nil, nil }
	_, err := unary(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x"}, h)
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("status = %v; want Unauthenticated", status.Code(err))
	}
	if called {
		t.Error("handler should not run when extractor errors")
	}
}

// TestNewServerInterceptors_E2E_EmbeddedPolicy stands up real gRPC
// servers behind the chained interceptors and exercises the embedded
// starter policy matrix end-to-end. The grpc-go authz RBAC engine
// reads the method + peer info from the framework, so an in-process
// gRPC server is the lowest-friction way to drive it.
func TestNewServerInterceptors_E2E_EmbeddedPolicy(t *testing.T) {
	cases := []struct {
		name       string
		principal  Principal
		wantDenied bool
	}{
		{"operator-allowed", Principal{Kind: "operator", Subject: "alice", Raw: "operator/alice"}, false},
		{"node-denied", Principal{Kind: "node", Subject: "1", Raw: "node/1"}, true},
		{"anonymous-denied", Principal{}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			unary, stream, closer, err := NewServerInterceptors(Config{
				Extractor: stubExtractor{p: c.principal},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if closer != nil {
					_ = closer()
				}
			}()

			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := grpc.NewServer(
				grpc.ChainUnaryInterceptor(unary),
				grpc.ChainStreamInterceptor(stream),
			)
			adminv1.RegisterAdminServer(srv, &fakeAdmin{})
			go srv.Serve(lis)
			defer func() { srv.Stop(); _ = lis.Close() }()

			cc, err := grpc.NewClient(lis.Addr().String(),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer cc.Close()
			_, err = adminv1.NewAdminClient(cc).ListNodes(context.Background(), &adminv1.ListNodesRequest{})
			if c.wantDenied {
				if status.Code(err) != codes.PermissionDenied {
					t.Errorf("got %v; want PermissionDenied", err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected err: %v", err)
				}
			}
		})
	}
}

// fakeAdmin satisfies just enough of AdminServer for the e2e test
// above. All RPCs return empty / unimplemented; the test only cares
// about whether the call reached the handler at all.
type fakeAdmin struct {
	adminv1.UnimplementedAdminServer
	mu sync.Mutex
}

func (f *fakeAdmin) ListNodes(_ context.Context, _ *adminv1.ListNodesRequest) (*adminv1.ListNodesResponse, error) {
	return &adminv1.ListNodesResponse{}, nil
}
