package server

import (
	"github.com/twinfer/reflow/pkg/sdk"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// buildDiscoveryResponse groups Entries by (service, kind) and collects
// every handler name under that group. Sorted-by-service-then-handler
// ordering falls out of Registry.Entries; we preserve it so the
// response is deterministic across registration-order shuffles.
func buildDiscoveryResponse(reg *sdk.Registry) *discoveryv1.DiscoveryResponse {
	entries := reg.Entries()
	// Group by (service, kind) preserving sort order.
	type key struct {
		service string
		kind    sdk.Kind
	}
	idx := make(map[key]int)
	out := make([]*discoveryv1.DiscoveredHandler, 0)
	for _, e := range entries {
		k := key{service: e.Service, kind: e.Kind}
		if i, ok := idx[k]; ok {
			out[i].HandlerNames = append(out[i].HandlerNames, e.Handler)
			continue
		}
		idx[k] = len(out)
		out = append(out, &discoveryv1.DiscoveredHandler{
			Service:      e.Service,
			Kind:         kindToProto(e.Kind),
			HandlerNames: []string{e.Handler},
		})
	}
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: protocolVersion,
		Handlers:        out,
	}
}

// protocolVersion is the wire version this server speaks. Must match
// the engine's admin.protocolVersion constant — bump both together
// when the wire contract changes.
const protocolVersion = "v1"

// kindToProto maps sdk.Kind to its protocolv1 wire enum. Kind is owned
// by the session protocol; discovery just echoes it.
func kindToProto(k sdk.Kind) protocolv1.Kind {
	switch k {
	case sdk.KindService:
		return protocolv1.Kind_KIND_SERVICE
	case sdk.KindObject:
		return protocolv1.Kind_KIND_OBJECT
	case sdk.KindWorkflow:
		return protocolv1.Kind_KIND_WORKFLOW
	default:
		return protocolv1.Kind_KIND_UNSPECIFIED
	}
}
