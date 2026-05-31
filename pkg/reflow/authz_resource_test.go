package reflow

import (
	"encoding/base64"
	"encoding/binary"
	"testing"

	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// TestIngressResourceTenant covers the security-critical half of tenant
// isolation: a by-id ingress request's resource must be attributed to the
// band encoded in its id (so a tenant/A caller presenting tenant/B's id is
// denied), recovered identically whether the id arrives as proto or string.
func TestIngressResourceTenant(t *testing.T) {
	pk := routing.PartitionKey(7, "svc", "key") // band 7
	id := &enginev1.InvocationId{PartitionKey: pk, Uuid: make([]byte, 16)}

	// proto form
	if got, ok := ingressResourceTenant("", &ingressv1.AwaitInvocationRequest{InvocationIdProto: id}); !ok || got != 7 {
		t.Errorf("proto by-id: got (%d,%v); want (7,true)", got, ok)
	}
	// string form — must recover the same band (no parser differential).
	if got, ok := ingressResourceTenant("", &ingressv1.CancelInvocationRequest{InvocationId: ingress.FormatInvocationID(id)}); !ok || got != 7 {
		t.Errorf("string by-id: got (%d,%v); want (7,true)", got, ok)
	}
	// awakeable id encodes the owner pk → owner's band.
	if got, ok := ingressResourceTenant("", &ingressv1.ResolveAwakeableRequest{AwakeableId: awakeableForOwner(t, pk)}); !ok || got != 7 {
		t.Errorf("awakeable: got (%d,%v); want (7,true)", got, ok)
	}

	// by-target requests have no id → resolver declines (interceptor falls back
	// to the principal's band, which is where by-target routes).
	if _, ok := ingressResourceTenant("", &ingressv1.SubmitInvocationRequest{Service: "svc"}); ok {
		t.Errorf("by-target Submit: resolver should decline")
	}
	if _, ok := ingressResourceTenant("", &ingressv1.GetObjectStateRequest{Service: "svc"}); ok {
		t.Errorf("by-target GetObjectState: resolver should decline")
	}
	// malformed id → decline (the handler rejects it as InvalidArgument).
	if _, ok := ingressResourceTenant("", &ingressv1.AwaitInvocationRequest{InvocationId: "not-an-id"}); ok {
		t.Errorf("malformed id: resolver should decline")
	}
}

// awakeableForOwner builds a valid awakeable id (awk_<base64url 16B body>,
// body = [8B owner pk][8B random]) so the resolver exercises real recovery via
// keys.AwakeableOwnerPartitionKey. Asserts the id is well-formed.
func awakeableForOwner(t *testing.T, pk uint64) string {
	t.Helper()
	body := make([]byte, 16)
	binary.BigEndian.PutUint64(body[:8], pk)
	id := "awk_" + base64.RawURLEncoding.EncodeToString(body)
	if err := keys.ValidateAwakeableID(id); err != nil {
		t.Fatalf("built malformed awakeable id %q: %v", id, err)
	}
	return id
}
