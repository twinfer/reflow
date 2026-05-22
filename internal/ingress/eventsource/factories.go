package eventsource

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Factory builds a (Subscriber, Publisher) pair for one configured
// source. Publisher is the DLQ sink — nil is allowed when DLQ.Topic is
// empty. Both are torn down via Close().
type Factory func(name string, backend BackendConfig, log *slog.Logger) (message.Subscriber, message.Publisher, error)

// Validator is the optional per-backend rule checker called from the
// admin RPC before a record is proposed. Returning a non-nil error
// surfaces as CodeInvalidArgument at upsert time — operators see the
// rule violation synchronously instead of a per-node reconcile
// failure deep in dispatcher logs.
//
// Validators check broker-imposed naming rules the generic record
// validation can't (Kafka topic legal chars, NATS JetStream stream-name
// constraint, SQS queue-name length, ...). nil registration means
// "no extra checks beyond the generic non-empty / unknown-type rules"
// — appropriate for backends like gochannel that have no broker
// naming surface.
type Validator func(topic string, backend BackendConfig) error

type registration struct {
	factory   Factory
	validator Validator
}

var (
	factoriesMu sync.RWMutex
	factories   = map[string]registration{}
)

// RegisterFactory installs a backend factory + optional Validator under
// the given type name. Called from each factory_*.go init(). Panics on
// duplicate registration. Pass nil for `v` when the backend has no
// extra naming rules.
func RegisterFactory(typeName string, f Factory, v Validator) {
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	if _, dup := factories[typeName]; dup {
		panic("eventsource: duplicate factory for type " + typeName)
	}
	factories[typeName] = registration{factory: f, validator: v}
}

func lookupFactory(typeName string) (Factory, error) {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	r, ok := factories[typeName]
	if !ok {
		return nil, fmt.Errorf("eventsource: unknown backend type %q", typeName)
	}
	return r.factory, nil
}

// HasFactory reports whether a factory is registered for the given
// backend type. Used by the admin Upsert RPC to reject a record that
// names an unknown type at validation time (so the operator gets a
// CodeInvalidArgument synchronously instead of a silent reconcile
// failure on every node).
func HasFactory(typeName string) bool {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	_, ok := factories[typeName]
	return ok
}

// Validate dispatches to the per-backend Validator registered for
// typeName. Unknown types and types registered without a Validator
// both return nil — the unknown-type case is the admin RPC's
// responsibility (it calls HasFactory before Validate) and the
// no-validator case is a deliberate "no extra rules" choice.
func Validate(typeName, topic string, backend BackendConfig) error {
	factoriesMu.RLock()
	r, ok := factories[typeName]
	factoriesMu.RUnlock()
	if !ok || r.validator == nil {
		return nil
	}
	return r.validator(topic, backend)
}

// RegisteredTypes returns the sorted list of registered factory type
// names. Useful for operator-facing error messages enumerating valid
// choices.
func RegisteredTypes() []string {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	out := make([]string, 0, len(factories))
	for name := range factories {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
