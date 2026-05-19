package eventsource

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus"
)

// Manager owns the eventsource dispatchers for the lifetime of a reflow
// Host. Construct with NewManager; call Run on a goroutine; Close on
// host shutdown.
type Manager struct {
	dispatchers []*Dispatcher
	subs        []message.Subscriber
	pubs        []message.Publisher
	metrics     *Metrics
	log         *slog.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
	mu     sync.Mutex
}

// NewManager validates the configuration and constructs one dispatcher
// per source. Returns nil when cfg.Sources is empty — caller checks
// for nil before invoking Run/Close. Any validation or factory error
// aborts; partial activation is not allowed.
func NewManager(cfg Config, submitter Submitter, reg prometheus.Registerer, log *slog.Logger) (*Manager, error) {
	if len(cfg.Sources) == 0 {
		return nil, nil
	}
	if submitter == nil {
		return nil, errors.New("eventsource: submitter is required")
	}
	if log == nil {
		log = slog.Default()
	}
	metrics := NewMetrics(reg)
	wmlog := watermillLogger(log)

	seen := make(map[string]struct{}, len(cfg.Sources))
	m := &Manager{metrics: metrics, log: log}
	for i := range cfg.Sources {
		sc := cfg.Sources[i]
		if err := validateSourceConfig(sc); err != nil {
			m.closeAll()
			return nil, fmt.Errorf("eventsource: source[%d] %q: %w", i, sc.Name, err)
		}
		if _, dup := seen[sc.Name]; dup {
			m.closeAll()
			return nil, fmt.Errorf("eventsource: duplicate source name %q", sc.Name)
		}
		seen[sc.Name] = struct{}{}

		factory, err := lookupFactory(sc.Type)
		if err != nil {
			m.closeAll()
			return nil, err
		}
		sub, pub, err := factory(sc.Name, sc.Backend, log)
		if err != nil {
			m.closeAll()
			return nil, fmt.Errorf("eventsource: build %q: %w", sc.Name, err)
		}
		sub, err = metrics.decorateSubscriber(sub)
		if err != nil {
			_ = sub.Close()
			if pub != nil {
				_ = pub.Close()
			}
			m.closeAll()
			return nil, fmt.Errorf("eventsource: decorate %q: %w", sc.Name, err)
		}
		m.subs = append(m.subs, sub)
		if pub != nil {
			m.pubs = append(m.pubs, pub)
		}

		objExtract, err := newExtractor(sc.ObjectKey.From, sc.ObjectKey.Value)
		if err != nil {
			m.closeAll()
			return nil, fmt.Errorf("eventsource: %q object_key: %w", sc.Name, err)
		}
		idemExtract, err := newExtractor(sc.Idempotency.From, sc.Idempotency.Value)
		if err != nil {
			m.closeAll()
			return nil, fmt.Errorf("eventsource: %q idempotency: %w", sc.Name, err)
		}

		d := &Dispatcher{
			name:        sc.Name,
			topic:       sc.Topic,
			service:     sc.Service,
			handler:     sc.Handler,
			objectKey:   objExtract,
			idempotency: idemExtract,
			sub:         sub,
			submitter:   submitter,
			metrics:     metrics,
			log:         log,
		}
		handle, err := compose(d.core(), sc, pub, wmlog)
		if err != nil {
			m.closeAll()
			return nil, fmt.Errorf("eventsource: compose %q: %w", sc.Name, err)
		}
		d.handle = handle
		m.dispatchers = append(m.dispatchers, d)
	}
	return m, nil
}

// Run blocks until ctx is cancelled or Close is called. Spawns one
// goroutine per dispatcher.
func (m *Manager) Run(ctx context.Context) {
	if m == nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()
	for _, d := range m.dispatchers {
		m.wg.Add(1)
		go func(d *Dispatcher) {
			defer m.wg.Done()
			if err := d.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				m.log.Error("eventsource: dispatcher exited", "source", d.name, "err", err)
			}
		}(d)
	}
	m.wg.Wait()
}

// Close cancels the run context, waits for dispatchers to drain, and
// closes every subscriber/publisher. Safe to call multiple times.
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	cancel := m.cancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
	return m.closeAll()
}

func (m *Manager) closeAll() error {
	var firstErr error
	for _, s := range m.subs {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, p := range m.pubs {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.subs = nil
	m.pubs = nil
	return firstErr
}

func validateSourceConfig(sc SourceConfig) error {
	if sc.Name == "" {
		return errors.New("name is required")
	}
	if sc.Type == "" {
		return errors.New("type is required")
	}
	if sc.Topic == "" {
		return errors.New("topic is required")
	}
	if sc.Service == "" {
		return errors.New("service is required")
	}
	if sc.Handler == "" {
		return errors.New("handler is required")
	}
	return nil
}
