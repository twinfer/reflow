// Package invoker hosts the leader-only goroutines that drive registered
// reflow handlers through their durable state machine. Phase 2 Step 10
// lands the scaffolding (types, lifecycle, transport, journal reader);
// Step 11 fills in the per-session goroutine.
package invoker

import (
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Registry adapts the public sdk.Registry to engine-side lookups keyed on
// enginev1.InvocationTarget. The engine carries routing info as the
// proto type; the SDK API expects sdk.Target.
type Registry struct {
	inner *sdk.Registry
}

// NewRegistry wraps the given sdk.Registry. A nil inner is accepted; all
// lookups then return (nil, false).
func NewRegistry(inner *sdk.Registry) *Registry {
	return &Registry{inner: inner}
}

// Lookup returns the handler for target's (ServiceName, HandlerName). The
// ObjectKey is ignored for handler lookup but is used elsewhere for
// routing.
func (r *Registry) Lookup(target *enginev1.InvocationTarget) (sdk.Handler, bool) {
	if r == nil || r.inner == nil || target == nil {
		return nil, false
	}
	return r.inner.Lookup(&sdk.Target{
		Service: target.GetServiceName(),
		Handler: target.GetHandlerName(),
		Key:     target.GetObjectKey(),
	})
}
