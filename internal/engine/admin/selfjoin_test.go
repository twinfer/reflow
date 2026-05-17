package admin

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twinfer/reflow/internal/auth"
)

// TestCheckSelfJoinPrincipal_* covers the SPIFFE-equals-NodeID gate
// SelfJoin enforces inside the handler (defense in depth behind the
// path-based grpc-go authz rule in starter_policy.json: even with the
// rule, a node/7 cert must not be able to register node 8).
//
// The full handler path (requireLeader → checkSelfJoinPrincipal →
// addNodeInternal) needs an engine.Host + MetadataRunner which are
// concrete dragonboat-backed types; the FSM body itself is exercised
// by TestMultiNode_JoinExistingCluster_OperatorAddNode in the engine
// integration suite. Here we pin the auth gate as a pure-function unit.

func TestCheckSelfJoinPrincipal_NoPrincipal(t *testing.T) {
	err := checkSelfJoinPrincipal(context.Background(), 4)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("want PermissionDenied, got %v", err)
	}
}

func TestCheckSelfJoinPrincipal_OperatorKindRejected(t *testing.T) {
	ctx := auth.ContextWithPrincipal(context.Background(), auth.Principal{
		Kind: "operator", Subject: "alice", Raw: "operator/alice",
	})
	err := checkSelfJoinPrincipal(ctx, 4)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("want PermissionDenied for operator principal, got %v", err)
	}
}

func TestCheckSelfJoinPrincipal_NodeIDMismatch(t *testing.T) {
	ctx := auth.ContextWithPrincipal(context.Background(), auth.Principal{
		Kind: "node", Subject: "7", Raw: "node/7",
	})
	err := checkSelfJoinPrincipal(ctx, 4)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("want PermissionDenied for node/7 calling SelfJoin(node_id=4), got %v", err)
	}
}

func TestCheckSelfJoinPrincipal_Match(t *testing.T) {
	ctx := auth.ContextWithPrincipal(context.Background(), auth.Principal{
		Kind: "node", Subject: "4", Raw: "node/4",
	})
	if err := checkSelfJoinPrincipal(ctx, 4); err != nil {
		t.Fatalf("want nil for matching node/4, got %v", err)
	}
}
