package handler

import (
	"context"

	connect "connectrpc.com/connect"

	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	"github.com/twinfer/reflow/proto/discoveryv1/discoveryv1connect"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// discoveryService implements discoveryv1connect.DiscoveryServiceHandler.
type discoveryService struct {
	discoveryv1connect.UnimplementedDiscoveryServiceHandler
	registry *Registry
}

func (d *discoveryService) Discover(_ context.Context, _ *connect.Request[discoveryv1.DiscoveryRequest]) (*connect.Response[discoveryv1.DiscoveryResponse], error) {
	return connect.NewResponse(buildDiscoveryResponse(d.registry)), nil
}

// buildDiscoveryResponse groups Entries by (service, kind) and collects
// every handler name under that group. Sorted-by-service-then-handler
// ordering falls out of Registry.Entries; we preserve it so the
// response is deterministic across registration-order shuffles.
func buildDiscoveryResponse(reg *Registry) *discoveryv1.DiscoveryResponse {
	entries := reg.Entries()
	// Group by (service, kind) preserving sort order.
	type key struct {
		service string
		kind    Kind
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

// kindToProto maps Kind to its protocolv1 wire enum. Kind is owned
// by the session protocol; discovery just echoes it.
func kindToProto(k Kind) protocolv1.Kind {
	switch k {
	case KindService:
		return protocolv1.Kind_KIND_SERVICE
	case KindObject:
		return protocolv1.Kind_KIND_OBJECT
	case KindWorkflow:
		return protocolv1.Kind_KIND_WORKFLOW
	case KindWorkflowShared:
		return protocolv1.Kind_KIND_WORKFLOW_SHARED
	default:
		return protocolv1.Kind_KIND_UNSPECIFIED
	}
}
