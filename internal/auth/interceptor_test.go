package auth

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// stubMapper returns canned (claims, err) regardless of input.
type stubMapper struct {
	claims *Claims
	err    error
}

func (s stubMapper) GetClaims(_ context.Context, _ AuthInfo) (*Claims, error) {
	return s.claims, s.err
}

// stubAuthz returns a canned Result. If err is non-nil it is returned
// instead and the Result is ignored.
type stubAuthz struct {
	res Result
	err error
}

func (s stubAuthz) Authorize(_ context.Context, _ *Claims, _ *CallTarget) (Result, error) {
	return s.res, s.err
}

func okHandler(invoked *bool) grpc.UnaryHandler {
	return func(_ context.Context, _ any) (any, error) {
		*invoked = true
		return "ok", nil
	}
}

func unaryInfo(method string) *grpc.UnaryServerInfo {
	return &grpc.UnaryServerInfo{FullMethod: method}
}

func TestUnaryInterceptor_AllowsAuthorizedCall(t *testing.T) {
	ic := UnaryInterceptor(
		stubMapper{claims: &Claims{Kind: "operator", Subject: "alice"}},
		stubAuthz{res: Result{Decision: DecisionAllow}},
		nil,
	)
	var called bool
	resp, err := ic(context.Background(), "req", unaryInfo("/svc/M"), okHandler(&called))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !called {
		t.Fatal("handler not invoked")
	}
	if resp != "ok" {
		t.Errorf("resp = %v; want ok", resp)
	}
}

func TestUnaryInterceptor_RejectsOnAuthzDeny(t *testing.T) {
	ic := UnaryInterceptor(
		stubMapper{claims: &Claims{Kind: "node"}},
		stubAuthz{res: Result{Decision: DecisionDeny, Reason: "wrong role"}},
		nil,
	)
	var called bool
	_, err := ic(context.Background(), "req", unaryInfo("/svc/M"), okHandler(&called))
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("status = %v; want PermissionDenied", status.Code(err))
	}
	if called {
		t.Error("handler should not run on deny")
	}
}

func TestUnaryInterceptor_RejectsOnMapperError(t *testing.T) {
	ic := UnaryInterceptor(
		stubMapper{err: errors.New("bad cert")},
		stubAuthz{}, nil,
	)
	var called bool
	_, err := ic(context.Background(), "req", unaryInfo("/svc/M"), okHandler(&called))
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("status = %v; want Unauthenticated", status.Code(err))
	}
	if called {
		t.Error("handler should not run on mapper error")
	}
}

func TestUnaryInterceptor_RejectsOnMissingIdentity(t *testing.T) {
	ic := UnaryInterceptor(
		stubMapper{}, // returns (nil, nil)
		stubAuthz{}, nil,
	)
	var called bool
	_, err := ic(context.Background(), "req", unaryInfo("/svc/M"), okHandler(&called))
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("status = %v; want Unauthenticated", status.Code(err))
	}
	if called {
		t.Error("handler should not run when no identity")
	}
}

func TestUnaryInterceptor_PutsClaimsOnContext(t *testing.T) {
	claims := &Claims{Kind: "operator", Subject: "alice"}
	ic := UnaryInterceptor(
		stubMapper{claims: claims},
		stubAuthz{res: Result{Decision: DecisionAllow}},
		nil,
	)
	var seen *Claims
	handler := func(ctx context.Context, _ any) (any, error) {
		seen, _ = ClaimsFromContext(ctx)
		return "ok", nil
	}
	if _, err := ic(context.Background(), "req", unaryInfo("/svc/M"), handler); err != nil {
		t.Fatal(err)
	}
	if seen != claims {
		t.Errorf("handler saw claims = %+v; want %+v", seen, claims)
	}
}

func TestUnaryInterceptor_PropagatesAuthorizerError(t *testing.T) {
	ic := UnaryInterceptor(
		stubMapper{claims: &Claims{Kind: "operator"}},
		stubAuthz{err: errors.New("policy backend offline")},
		nil,
	)
	var called bool
	_, err := ic(context.Background(), "req", unaryInfo("/svc/M"), okHandler(&called))
	if status.Code(err) != codes.Internal {
		t.Errorf("status = %v; want Internal", status.Code(err))
	}
	if called {
		t.Error("handler should not run on authorizer error")
	}
}

// fakeServerStream is a minimal grpc.ServerStream implementation for
// driving StreamInterceptor in tests.
type fakeServerStream struct {
	ctx context.Context
}

func (f *fakeServerStream) SetHeader(_ metadata.MD) error  { return nil }
func (f *fakeServerStream) SendHeader(_ metadata.MD) error { return nil }
func (f *fakeServerStream) SetTrailer(_ metadata.MD)       {}
func (f *fakeServerStream) Context() context.Context       { return f.ctx }
func (f *fakeServerStream) SendMsg(_ any) error            { return nil }
func (f *fakeServerStream) RecvMsg(_ any) error            { return nil }

func TestStreamInterceptor_AllowsAuthorizedStream(t *testing.T) {
	ic := StreamInterceptor(
		stubMapper{claims: &Claims{Kind: "node", Subject: "3"}},
		stubAuthz{res: Result{Decision: DecisionAllow}},
		nil,
	)
	var seen *Claims
	handler := func(_ any, ss grpc.ServerStream) error {
		seen, _ = ClaimsFromContext(ss.Context())
		return nil
	}
	info := &grpc.StreamServerInfo{FullMethod: "/reflow.delivery.v1.Delivery/Deliver"}
	err := ic(nil, &fakeServerStream{ctx: context.Background()}, info, handler)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if seen == nil || seen.Kind != "node" {
		t.Errorf("handler saw claims = %+v; want node claim", seen)
	}
}

func TestStreamInterceptor_RejectsOnAuthzDeny(t *testing.T) {
	ic := StreamInterceptor(
		stubMapper{claims: &Claims{Kind: "operator"}},
		stubAuthz{res: Result{Decision: DecisionDeny, Reason: "delivery wants node"}},
		nil,
	)
	var called bool
	handler := func(_ any, _ grpc.ServerStream) error { called = true; return nil }
	info := &grpc.StreamServerInfo{FullMethod: "/reflow.delivery.v1.Delivery/Deliver"}
	err := ic(nil, &fakeServerStream{ctx: context.Background()}, info, handler)
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("status = %v; want PermissionDenied", status.Code(err))
	}
	if called {
		t.Error("handler should not run on deny")
	}
}
