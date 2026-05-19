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
