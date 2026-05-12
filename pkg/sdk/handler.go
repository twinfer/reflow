package sdk

import (
	"fmt"
	"sync"
)

// Handler is the user-supplied function the engine invokes when a request
// reaches a registered (service, handler) pair. The Context argument
// supplies every durable-execution primitive; input is the payload that
// was passed to SubmitInvocation.
//
// Returning a *Failure terminates the invocation with the failure
// preserved in the journal. Returning any other error is treated as
// transient and triggers a retry.
type Handler func(ctx Context, input []byte) ([]byte, error)

// Registry holds the durable handlers a node is willing to run.
//
// Construct one with NewRegistry, then call Register for each
// (service, handler) pair before passing it to reflow.Run. Register is
// concurrency-safe, but registration after Run starts is not visible to
// in-flight invocations — the engine snapshots the lookup at session
// startup time.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register adds fn under (service, handler). Both names must be
// non-empty, fn must be non-nil, and the pair must not already be
// registered. Returns an error on any violation so callers can detect
// configuration bugs without recover.
func (r *Registry) Register(service, handler string, fn Handler) error {
	if service == "" {
		return fmt.Errorf("sdk: Register: service must be non-empty")
	}
	if handler == "" {
		return fmt.Errorf("sdk: Register: handler must be non-empty")
	}
	if fn == nil {
		return fmt.Errorf("sdk: Register: fn must be non-nil for %s/%s", service, handler)
	}
	key := service + "/" + handler
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handlers == nil {
		r.handlers = make(map[string]Handler)
	}
	if _, dup := r.handlers[key]; dup {
		return fmt.Errorf("sdk: Register: handler %s already registered", key)
	}
	r.handlers[key] = fn
	return nil
}

// Lookup returns the handler registered for target's (Service, Handler).
// The Key field is ignored — routing uses it but handler lookup does not.
func (r *Registry) Lookup(target *Target) (Handler, bool) {
	if target == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[target.Service+"/"+target.Handler]
	return h, ok
}

// Len returns the number of registered handlers. Tests use this to
// confirm registration before Run.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}
