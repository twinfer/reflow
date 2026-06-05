package authz

import (
	"context"
	"log/slog"
	"testing"

	"github.com/cedar-policy/cedar-go"

	"github.com/twinfer/reflw/internal/auth"
)

// testLog discards reconcile-loop warnings so failing-path tests don't spam
// output (reconcileOnce logs + swallows; it never propagates).
var testLog = slog.New(slog.DiscardHandler)

type fakeReader struct {
	text string
	rev  uint64
	err  error
}

func (f fakeReader) ClusterAuthzPolicy(_ context.Context) (string, uint64, error) {
	return f.text, f.rev, f.err
}

// anonIngress returns the engine's decision for an anonymous ingress call
// (AwaitInvocation, a representative IngressActions procedure) — Allow under
// the foundational (open-ingress) set, Deny under an operator-only set. Used to
// observe which policy set is live. (SubmitInvocation moved to the REST facade
// and is no longer a Connect procedure; its Cedar action is unchanged.)
func anonIngress(e *Engine) cedar.Decision {
	return evalReq(e, "/reflw.ingress.v1.Ingress/AwaitInvocation",
		auth.Principal{}, TypeInvocation, "svc", nil)
}

// TestReconcileOnce_EmptyKeepsFoundational proves a fresh cluster (empty row)
// leaves the in-binary foundational set installed — never clobbered with an
// empty policy that would deny everything.
func TestReconcileOnce_EmptyKeepsFoundational(t *testing.T) {
	e := mustEngine(t, FoundationalClusterPolicies)
	e.reconcileOnce(context.Background(), fakeReader{text: ""}, testLog)
	if got := anonIngress(e); got != cedar.Allow {
		t.Fatalf("empty reconcile clobbered foundational; anon ingress = %v want Allow", got)
	}
}

// TestReconcileOnce_ValidPolicySwaps proves a valid uploaded policy replaces
// the live set: an operator-only policy makes anonymous ingress Deny.
func TestReconcileOnce_ValidPolicySwaps(t *testing.T) {
	e := mustEngine(t, FoundationalClusterPolicies)
	if got := anonIngress(e); got != cedar.Allow {
		t.Fatalf("precondition: anon ingress = %v want Allow", got)
	}
	e.reconcileOnce(context.Background(),
		fakeReader{text: `permit (principal is ClusterOperator, action, resource);`, rev: 1}, testLog)
	if got := anonIngress(e); got != cedar.Deny {
		t.Fatalf("operator-only policy not applied; anon ingress = %v want Deny", got)
	}
}

// TestReconcileOnce_InvalidKeepsPrevious proves a policy that fails to compile
// is rejected and the previous set is kept — a bad reconcile can neither open
// the cluster up nor lock it out.
func TestReconcileOnce_InvalidKeepsPrevious(t *testing.T) {
	e := mustEngine(t, FoundationalClusterPolicies)
	e.reconcileOnce(context.Background(),
		fakeReader{text: `permit (principal is Bogus, action, resource);`, rev: 2}, testLog)
	if got := anonIngress(e); got != cedar.Allow {
		t.Fatalf("invalid policy was applied; anon ingress = %v want Allow (foundational kept)", got)
	}
}

// TestReconcileOnce_ReadErrorKeepsPrevious proves a transient read failure is
// swallowed and the current set survives.
func TestReconcileOnce_ReadErrorKeepsPrevious(t *testing.T) {
	e := mustEngine(t, FoundationalClusterPolicies)
	e.reconcileOnce(context.Background(),
		fakeReader{err: context.DeadlineExceeded}, testLog)
	if got := anonIngress(e); got != cedar.Allow {
		t.Fatalf("read error disturbed live set; anon ingress = %v want Allow", got)
	}
}

// TestValidateClusterPolicy is the upload gate the Config server applies: a
// schema-conformant policy passes; an appliesTo violation (Anonymous on the
// operator-only AddNode) is rejected at upload, never installed.
func TestValidateClusterPolicy(t *testing.T) {
	if err := ValidateClusterPolicy([]byte(FoundationalClusterPolicies)); err != nil {
		t.Errorf("foundational policy should validate: %v", err)
	}
	bad := `permit (principal is Anonymous, action == Action::"AddNode", resource);`
	if err := ValidateClusterPolicy([]byte(bad)); err == nil {
		t.Error("expected appliesTo violation to be rejected at upload")
	}
}
