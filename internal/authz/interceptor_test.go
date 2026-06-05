package authz

import (
	"context"
	"errors"
	"testing"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/auth"
)

// TestInterceptor_Authorize covers the enforcement decision + the
// anonymous(401) vs known-principal(403) split that downstream clients rely
// on. It exercises authorize directly (the WrapUnary/WrapStreamingHandler
// wrappers are thin forwarders to it).
func TestInterceptor_Authorize(t *testing.T) {
	e := mustEngine(t, FoundationalClusterPolicies)
	ic := NewInterceptor(e, nil, true)

	ctx := func(p auth.Principal) context.Context {
		return auth.ContextWithPrincipal(context.Background(), p)
	}
	operator := auth.Principal{Kind: "operator", Subject: "alice"}
	node := auth.Principal{Kind: "node", Subject: "3"}

	cases := []struct {
		name      string
		ctx       context.Context
		procedure string
		wantCode  connect.Code // 0 => allowed (nil error)
	}{
		{"operator-config-allow", ctx(operator), actRegisterDeployment, 0},
		{"operator-addnode-allow", ctx(operator), actAddNode, 0},
		{"node-deliver-allow", ctx(node), actDeliver, 0},
		{"node-selfjoin-allow", ctx(node), actSelfJoin, 0},
		{"node-config-denied", ctx(node), actRegisterDeployment, connect.CodePermissionDenied},
		{"anon-await-open", context.Background(), actAwaitInvocation, 0},
		{"anon-config-unauth", context.Background(), actRegisterDeployment, connect.CodeUnauthenticated},
		{"anon-addnode-unauth", context.Background(), actAddNode, connect.CodeUnauthenticated},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ic.authorize(c.ctx, c.procedure)
			if c.wantCode == 0 {
				if err != nil {
					t.Fatalf("got error %v; want allow", err)
				}
				return
			}
			if got := connect.CodeOf(err); got != c.wantCode {
				t.Errorf("code = %v; want %v (err=%v)", got, c.wantCode, err)
			}
		})
	}
}

// TestInterceptor_AnonymousChallenge confirms the WWW-Authenticate: Bearer
// hint rides anonymous denials only when bearer auth is enabled.
func TestInterceptor_AnonymousChallenge(t *testing.T) {
	e := mustEngine(t, FoundationalClusterPolicies)
	anon := context.Background()

	var ce *connect.Error

	withBearer := NewInterceptor(e, nil, true)
	if err := withBearer.authorize(anon, actRegisterDeployment); !errors.As(err, &ce) || ce.Meta().Get("WWW-Authenticate") != "Bearer" {
		t.Errorf("bearer-enabled: missing WWW-Authenticate challenge (err=%v)", err)
	}

	noBearer := NewInterceptor(e, nil, false)
	if err := noBearer.authorize(anon, actRegisterDeployment); errors.As(err, &ce) && ce.Meta().Get("WWW-Authenticate") != "" {
		t.Errorf("bearer-disabled: unexpected WWW-Authenticate challenge")
	}
}
