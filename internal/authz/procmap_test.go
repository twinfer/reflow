package authz

import (
	"testing"

	"github.com/cedar-policy/cedar-go"
	"google.golang.org/protobuf/reflect/protoreflect"

	clusterctlv1 "github.com/twinfer/reflw/proto/clusterctlv1"
	configv1 "github.com/twinfer/reflw/proto/configv1"
	deliveryv1 "github.com/twinfer/reflw/proto/deliveryv1"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// inScopeFiles are the four Connect services mounted behind the authz
// interceptor. bootstrap.MeshSign, discovery, and handler.HandlerService live
// on separate listeners and are intentionally excluded.
var inScopeFiles = []protoreflect.FileDescriptor{
	ingressv1.File_ingressv1_ingress_proto,
	configv1.File_configv1_config_proto,
	clusterctlv1.File_clusterctlv1_clusterctl_proto,
	deliveryv1.File_deliveryv1_delivery_proto,
}

// allProcedures enumerates "/<service>/<method>" for every method of every
// service defined in the in-scope files, via proto reflection.
func allProcedures() []string {
	var out []string
	for _, fd := range inScopeFiles {
		svcs := fd.Services()
		for i := 0; i < svcs.Len(); i++ {
			svc := svcs.Get(i)
			methods := svc.Methods()
			for j := 0; j < methods.Len(); j++ {
				out = append(out, "/"+string(svc.FullName())+"/"+string(methods.Get(j).Name()))
			}
		}
	}
	return out
}

// TestProcMap_CoversEveryInScopeProcedure asserts procMap has an entry for
// every RPC on the four interceptor-guarded services and carries no stale
// entries — so a newly-added RPC fails tests until it is classified into a
// plane (no silent authz gap), and a removed RPC can't leave a dead mapping.
func TestProcMap_CoversEveryInScopeProcedure(t *testing.T) {
	want := map[string]bool{}
	for _, p := range allProcedures() {
		want[p] = true
		if _, ok := procMap[p]; !ok {
			t.Errorf("procMap missing entry for in-scope procedure %s", p)
		}
	}
	for p := range procMap {
		if !want[p] {
			t.Errorf("procMap has stale entry %s (no such in-scope procedure)", p)
		}
	}
}

// TestProcMap_ActionIDsUnique asserts the bare-method action ids don't collide
// across services — the property that lets a policy say Action::"<method>"
// with no service prefix and stay unambiguous.
func TestProcMap_ActionIDsUnique(t *testing.T) {
	seen := map[string]string{}
	for proc, e := range procMap {
		if prev, dup := seen[e.action]; dup {
			t.Errorf("action id %q maps from both %s and %s", e.action, prev, proc)
		}
		seen[e.action] = proc
	}
}

// TestActionEntity_StampsGroupParents proves actionEntity attaches the plane
// group as a parent edge (so `action in [Action::"<group>"]` matches at eval)
// and reports ok=false for an unmapped procedure (interceptor default-deny).
func TestActionEntity_StampsGroupParents(t *testing.T) {
	uid, ent, ok := actionEntity("/reflw.delivery.v1.Delivery/Deliver")
	if !ok {
		t.Fatal("Deliver should be mapped")
	}
	if string(uid.ID) != "Deliver" {
		t.Errorf("action id = %q want Deliver", uid.ID)
	}
	mesh := cedar.NewEntityUID(actionType, "MeshActions")
	if !ent.Parents.Contains(mesh) {
		t.Errorf("Deliver entity missing MeshActions parent edge")
	}
	if _, _, ok := actionEntity("/reflw.unknown.v1.Svc/Nope"); ok {
		t.Error("unmapped procedure should report ok=false")
	}
}
