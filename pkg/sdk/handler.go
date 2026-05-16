package sdk

import (
	"fmt"
	"sort"
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

// Kind tags how a service is addressed and how its lifecycle is shaped.
// Mirrors Restate's service kinds (service / virtual object / workflow)
// and rides on the wire StartMessage.kind field in protocolv1.
//
//   - KindService:  unkeyed, per-invocation, stateless.
//   - KindObject:   addressed by (service_name, object_key); per-invocation
//     but locked per key so concurrent invocations for the
//     same key serialise. Per-key durable state.
//   - KindWorkflow: addressed by (workflow_name, workflow_key); one run
//     per (name, key), long-lived. Per-key state + named
//     promises.
//
// Per-kind lifecycle differences (object locking, workflow replay
// scoping) are still being wired; today every kind dispatches the same
// wire path. Kind is surfaced now so deployment registration and the
// wire StartMessage stay stable.
type Kind int

const (
	// KindUnspecified is the zero value. Lookup returns it alongside ok=false.
	KindUnspecified Kind = iota
	KindService
	KindObject
	KindWorkflow
)

// String returns a short human-readable form.
func (k Kind) String() string {
	switch k {
	case KindService:
		return "service"
	case KindObject:
		return "object"
	case KindWorkflow:
		return "workflow"
	default:
		return "unspecified"
	}
}

// Registry holds the durable handlers a node is willing to run.
//
// Construct one with NewRegistry, then call RegisterService /
// RegisterObject / RegisterWorkflow for each (service, handler) pair
// before passing it to reflow.Run. Registration is concurrency-safe, but
// registration after Run starts is not visible to in-flight invocations
// — the engine snapshots the lookup at session startup time.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]registration
}

// registration pairs a handler with its kind so Lookup can return both in
// one shot.
type registration struct {
	fn   Handler
	kind Kind
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]registration)}
}

// RegisterService adds fn under (service, handler) as a stateless,
// unkeyed service handler. Returns an error on bad inputs or duplicate
// registration.
func (r *Registry) RegisterService(service, handler string, fn Handler) error {
	return r.register(service, handler, fn, KindService)
}

// RegisterObject adds fn under (service, handler) as a virtual-object
// handler. The handler will be addressed as (service, object_key) and
// runs locked per key.
func (r *Registry) RegisterObject(service, handler string, fn Handler) error {
	return r.register(service, handler, fn, KindObject)
}

// RegisterWorkflow adds fn under (service, handler) as a workflow
// handler. The handler will be addressed as (workflow_name, workflow_key)
// with one durable run per key.
func (r *Registry) RegisterWorkflow(service, handler string, fn Handler) error {
	return r.register(service, handler, fn, KindWorkflow)
}

func (r *Registry) register(service, handler string, fn Handler, kind Kind) error {
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
		r.handlers = make(map[string]registration)
	}
	if _, dup := r.handlers[key]; dup {
		return fmt.Errorf("sdk: Register: handler %s already registered", key)
	}
	r.handlers[key] = registration{fn: fn, kind: kind}
	return nil
}

// Lookup returns the handler registered for target's (Service, Handler)
// plus its Kind. The Key field is ignored — routing uses it but handler
// lookup does not. When ok is false the returned Handler is nil and Kind
// is KindUnspecified.
func (r *Registry) Lookup(target *Target) (Handler, Kind, bool) {
	if target == nil {
		return nil, KindUnspecified, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	reg, ok := r.handlers[target.Service+"/"+target.Handler]
	if !ok {
		return nil, KindUnspecified, false
	}
	return reg.fn, reg.kind, true
}

// Len returns the number of registered handlers. Tests use this to
// confirm registration before Run.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}

// Entry is one registered (service, handler, kind) tuple. Returned by
// Entries in lexicographic order so callers can build deterministic
// digests of the registry.
type Entry struct {
	Service string
	Handler string
	Kind    Kind
}

// Entries returns every registered handler as a slice sorted by
// "service/handler" ascending. The sort is what makes deployment_id
// hashes stable across registration-order shuffles.
func (r *Registry) Entries() []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Entry, 0, len(r.handlers))
	for key, reg := range r.handlers {
		// key is exactly "service/handler" — split on the last slash so
		// service names containing '/' (forbidden by Register) still
		// don't surprise us here.
		slash := -1
		for i := len(key) - 1; i >= 0; i-- {
			if key[i] == '/' {
				slash = i
				break
			}
		}
		if slash < 0 {
			continue
		}
		out = append(out, Entry{
			Service: key[:slash],
			Handler: key[slash+1:],
			Kind:    reg.kind,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Service != out[j].Service {
			return out[i].Service < out[j].Service
		}
		return out[i].Handler < out[j].Handler
	})
	return out
}
