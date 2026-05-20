package eventsource

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Factory builds a (Subscriber, Publisher) pair for one configured
// source. Publisher is the DLQ sink — nil is allowed when DLQ.Topic is
// empty. Both are torn down via Close().
type Factory func(name string, backend BackendConfig, log *slog.Logger) (message.Subscriber, message.Publisher, error)

var (
	factoriesMu sync.RWMutex
	factories   = map[string]Factory{}
)

// RegisterFactory installs a backend factory under the given type name.
// Called from each factory_*.go init(). Panics on duplicate registration.
func RegisterFactory(typeName string, f Factory) {
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	if _, dup := factories[typeName]; dup {
		panic("eventsource: duplicate factory for type " + typeName)
	}
	factories[typeName] = f
}

func lookupFactory(typeName string) (Factory, error) {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	f, ok := factories[typeName]
	if !ok {
		return nil, fmt.Errorf("eventsource: unknown backend type %q", typeName)
	}
	return f, nil
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
	// stable order without sorting import — small list, insertion sort.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
