package authz

import (
	"regexp"
	"testing"

	"github.com/cedar-policy/cedar-go"
	"google.golang.org/protobuf/reflect/protoreflect"

	adminv1 "github.com/twinfer/reflw/proto/adminv1"
	deliveryv1 "github.com/twinfer/reflw/proto/deliveryv1"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// inScopeFiles are the three Connect services mounted behind the authz
// interceptor. discovery and handler.HandlerService live on separate listeners
// and are intentionally excluded.
var inScopeFiles = []protoreflect.FileDescriptor{
	ingressv1.File_ingressv1_ingress_proto,
	adminv1.File_adminv1_admin_proto,
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

// schemaActionRE matches a quoted action declaration in schema.cedar
// (`action "AwaitInvocation" ...`), capturing the bare action id. Group
// declarations (`action IngressActions;`) are unquoted and so excluded.
var schemaActionRE = regexp.MustCompile(`action\s+"([^"]+)"`)

// TestSchemaActions_MatchProcmap closes the drift gap that let dead actions
// (the removed CA-root / join-token surface) and missing actions (the model and
// process-plane RPCs) sit in schema.cedar undetected: TestProcMap_* only checks
// procMap<->RPC, never schema<->procMap. This asserts both directions — every
// procMap action is declared in the schema, and every schema action has a
// procMap entry (every action is a real Connect procedure now that the REST
// facade is gone) — so a future add/remove that touches only one side fails CI.
func TestSchemaActions_MatchProcmap(t *testing.T) {
	schemaActions := map[string]bool{}
	for _, m := range schemaActionRE.FindAllStringSubmatch(string(schemaText), -1) {
		schemaActions[m[1]] = true
	}
	procActions := map[string]bool{}
	for _, e := range procMap {
		procActions[e.action] = true
	}
	for a := range procActions {
		if !schemaActions[a] {
			t.Errorf("procMap action %q is not declared in schema.cedar", a)
		}
	}
	for a := range schemaActions {
		if !procActions[a] {
			t.Errorf("schema.cedar action %q has no procMap entry", a)
		}
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
