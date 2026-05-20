package eventsource

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus"
)

// reconcileLabelTable is the table label every reconcile metric is
// stamped with. Kept as a constant so Phase C (webhooks) doesn't accidentally
// stomp the eventsource series; webhooks will use their own label.
const reconcileLabelTable = "eventsrc"

// drainTimeout caps how long Reconcile waits for a removed source's
// in-flight SubmitInvocation calls to complete before tearing down its
// subscriber and goroutine. Tunable per call via Reconcile.
const drainTimeout = 5 * time.Second

// reconcileInterval is the backstop tick — even if the FSM-driven
// TableNotifier signal is lost (engine restart mid-apply, panic in a
// prior reconcile), the loop re-reads shard 0 every interval. Short
// enough that operators don't notice a stuck reconcile for long; long
// enough that the SyncRead traffic stays trivial.
const reconcileInterval = 5 * time.Second

// Reader is the seam the reconcile loop uses to fetch desired state.
// Production wiring is a thin adapter over engine.Host.EventSources;
// tests can hand in a fake.
type Reader interface {
	ListEventSources(ctx context.Context) ([]SourceConfig, uint64 /*tableRev*/, error)
}

// Manager owns the per-source dispatchers for the lifetime of a reflow
// Host. The dispatcher set is mutated only by Reconcile (and Close);
// Run blocks until ctx is cancelled.
//
// Two call patterns:
//
//   - Static, one-shot: NewManager(cfg, ...) seeds the dispatcher set
//     synchronously; Run blocks; Close drains. Existing integration
//     tests use this shape.
//
//   - Dynamic reconcile: New(...) builds an empty Manager;
//     RunReconciler(ctx, sub, reader) becomes the long-running loop
//     that wakes on sub or a 5s ticker, SyncRead's the table via
//     reader, and Reconciles. Production (pkg/reflow/run.go) uses
//     this shape.
type Manager struct {
	submitter Submitter
	wmlog     watermill.LoggerAdapter
	metrics   *Metrics
	log       *slog.Logger

	mu          sync.Mutex
	dispatchers map[string]*dispatcherEntry
	closed      bool
	cancel      context.CancelFunc
}

// dispatcherEntry pairs a live Dispatcher with its subscriber/publisher
// and an effective copy of the SourceConfig used to build them, so
// Reconcile can detect "config changed" by comparing the new desired
// SourceConfig against the entry's previous one.
type dispatcherEntry struct {
	cfg    SourceConfig
	disp   *Dispatcher
	sub    message.Subscriber
	pub    message.Publisher
	cancel context.CancelFunc
	done   chan struct{}
}

// New constructs an empty Manager. Use Reconcile to populate the
// dispatcher set; use RunReconciler for the production wake-on-notifier
// loop, or call Reconcile directly from tests.
func New(submitter Submitter, reg prometheus.Registerer, log *slog.Logger) (*Manager, error) {
	if submitter == nil {
		return nil, errors.New("eventsource: submitter is required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		submitter:   submitter,
		wmlog:       watermillLogger(log),
		metrics:     NewMetrics(reg),
		log:         log,
		dispatchers: make(map[string]*dispatcherEntry),
	}, nil
}

// NewManager is the static-config convenience: construct a Manager and
// synchronously seed it from cfg.Sources. Returns (nil, nil) when
// cfg.Sources is empty so callers can keep their "if mgr == nil skip
// Run" check.
//
// Validation, factory lookup, and dispatcher start all happen here; a
// failure on any source aborts and tears down everything started so
// far. After this returns successfully the dispatchers are already
// running — Run is only useful to block until shutdown.
func NewManager(cfg Config, submitter Submitter, reg prometheus.Registerer, log *slog.Logger) (*Manager, error) {
	if len(cfg.Sources) == 0 {
		return nil, nil
	}
	m, err := New(submitter, reg, log)
	if err != nil {
		return nil, err
	}
	if err := m.Reconcile(context.Background(), cfg.Sources); err != nil {
		_ = m.Close()
		return nil, err
	}
	return m, nil
}

// Run blocks until ctx is cancelled or Close is called. Used by tests
// (and the static-config production path) that already seeded the
// dispatcher set via NewManager / Reconcile. Production code on the
// reconcile path should use RunReconciler instead.
func (m *Manager) Run(ctx context.Context) {
	if m == nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()
	<-ctx.Done()
}

// RunReconciler is the production-mode reconcile loop. It wakes on
// either:
//
//   - sub: a fan-out signal from the FSM apply path (cluster.TableNotifier
//     post-commit Bump). The channel is buffered-1 in the notifier, so
//     receipt is best-effort — the loop's response is always a fresh
//     SyncRead, never a per-event diff.
//   - reconcileInterval ticker: backstop, in case sub is dropped.
//
// Each wake calls reader.ListEventSources and then Reconcile. Errors
// from either are logged + counted on reflow_config_reconcile_errors_total
// and do not stop the loop. Returns ctx.Err() on cancellation.
//
// Goroutine affinity: own dedicated goroutine. Does NOT run on the
// FSM apply path. Per internal/engine/CLAUDE.md, the FSM apply path
// must never block on Raft propose / external RPC, so the reconcile
// work lives off-loop and the apply-path TableNotifier.Bump is a
// non-blocking fan-out signal only.
func (m *Manager) RunReconciler(ctx context.Context, sub <-chan struct{}, reader Reader) error {
	if m == nil {
		return nil
	}
	if reader == nil {
		return errors.New("eventsource: reader is required for reconcile loop")
	}
	ctx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	// Kick once at start so a fresh Manager picks up the current table
	// state without waiting for either the first notifier wake or the
	// first ticker.
	m.reconcileOnce(ctx, reader)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub:
			m.reconcileOnce(ctx, reader)
		case <-ticker.C:
			m.reconcileOnce(ctx, reader)
		}
	}
}

func (m *Manager) reconcileOnce(ctx context.Context, reader Reader) {
	desired, rev, err := reader.ListEventSources(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			m.log.Warn("eventsource: read desired state", "err", err)
			m.metrics.ReconcileErrors.WithLabelValues(reconcileLabelTable, "*").Inc()
		}
		return
	}
	if err := m.Reconcile(ctx, desired); err != nil {
		m.log.Warn("eventsource: reconcile failed", "err", err)
	}
	m.metrics.TableRevision.WithLabelValues(reconcileLabelTable).Set(float64(rev))
}

// Reconcile drives the dispatcher set toward `desired`. Add/start
// added, drain+stop removed, restart changed. Errors on a single source
// are logged + counted on reflow_config_reconcile_errors_total{key=<name>}
// and do not abort the rest of the reconcile (so a single bad-config
// source can't break unrelated active ones).
func (m *Manager) Reconcile(ctx context.Context, desired []SourceConfig) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("eventsource: manager is closed")
	}
	// Snapshot current under lock; mutations happen one-by-one with the
	// lock dropped during drain so concurrent Reconciles serialize on
	// add/remove but never block on graceful drains.
	current := make(map[string]*dispatcherEntry, len(m.dispatchers))
	maps.Copy(current, m.dispatchers)
	seen := make(map[string]struct{}, len(desired))
	for _, sc := range desired {
		seen[sc.Name] = struct{}{}
	}
	m.mu.Unlock()

	// Removals.
	for name := range current {
		if _, keep := seen[name]; keep {
			continue
		}
		if err := m.stopDispatcher(name); err != nil {
			m.log.Warn("eventsource: stop dispatcher", "source", name, "err", err)
			m.metrics.ReconcileErrors.WithLabelValues(reconcileLabelTable, name).Inc()
		}
	}

	// Additions and updates.
	for _, sc := range desired {
		existing, has := current[sc.Name]
		if has && sourceConfigsEqual(existing.cfg, sc) {
			continue
		}
		if has {
			// Changed — stop+start so the new config takes effect. A
			// per-source change does not require draining peers.
			if err := m.stopDispatcher(sc.Name); err != nil {
				m.log.Warn("eventsource: stop for replace", "source", sc.Name, "err", err)
				m.metrics.ReconcileErrors.WithLabelValues(reconcileLabelTable, sc.Name).Inc()
				continue
			}
		}
		if err := m.startDispatcher(ctx, sc); err != nil {
			m.log.Warn("eventsource: start dispatcher", "source", sc.Name, "err", err)
			m.metrics.ReconcileErrors.WithLabelValues(reconcileLabelTable, sc.Name).Inc()
		}
	}
	return nil
}

// startDispatcher builds and spawns one dispatcher. Errors here are
// caller-handled; nothing is leaked because we wire subscriber/publisher
// only after every error-prone build step has succeeded.
func (m *Manager) startDispatcher(ctx context.Context, sc SourceConfig) error {
	if err := validateSourceConfig(sc); err != nil {
		return fmt.Errorf("source %q: %w", sc.Name, err)
	}
	factory, err := lookupFactory(sc.Type)
	if err != nil {
		return err
	}
	sub, pub, err := factory(sc.Name, sc.Backend, m.log)
	if err != nil {
		return fmt.Errorf("build %q: %w", sc.Name, err)
	}
	sub, err = m.metrics.decorateSubscriber(sub)
	if err != nil {
		_ = sub.Close()
		if pub != nil {
			_ = pub.Close()
		}
		return fmt.Errorf("decorate %q: %w", sc.Name, err)
	}
	objExtract, err := newExtractor(sc.ObjectKey.From, sc.ObjectKey.Value)
	if err != nil {
		_ = sub.Close()
		if pub != nil {
			_ = pub.Close()
		}
		return fmt.Errorf("%q object_key: %w", sc.Name, err)
	}
	idemExtract, err := newExtractor(sc.Idempotency.From, sc.Idempotency.Value)
	if err != nil {
		_ = sub.Close()
		if pub != nil {
			_ = pub.Close()
		}
		return fmt.Errorf("%q idempotency: %w", sc.Name, err)
	}
	d := &Dispatcher{
		name:        sc.Name,
		topic:       sc.Topic,
		service:     sc.Service,
		handler:     sc.Handler,
		objectKey:   objExtract,
		idempotency: idemExtract,
		sub:         sub,
		submitter:   m.submitter,
		metrics:     m.metrics,
		log:         m.log,
	}
	handle, err := compose(d.core(), sc, pub, m.wmlog)
	if err != nil {
		_ = sub.Close()
		if pub != nil {
			_ = pub.Close()
		}
		return fmt.Errorf("compose %q: %w", sc.Name, err)
	}
	d.handle = handle

	// Dispatcher ctx is a child of the caller's so Reconcile-loop
	// cancellation propagates. NewManager calls Reconcile with
	// context.Background which keeps dispatchers bound only to Close;
	// RunReconciler passes its own loop context which is cancelled at
	// Close. Per-dispatcher stopDispatcher uses dcancel for individual
	// removals without touching peers.
	dctx, dcancel := context.WithCancel(ctx)
	entry := &dispatcherEntry{
		cfg:    sc,
		disp:   d,
		sub:    sub,
		pub:    pub,
		cancel: dcancel,
		done:   make(chan struct{}),
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		dcancel()
		_ = sub.Close()
		if pub != nil {
			_ = pub.Close()
		}
		return errors.New("eventsource: manager is closed")
	}
	m.dispatchers[sc.Name] = entry
	m.mu.Unlock()
	go func() {
		defer close(entry.done)
		if err := d.Run(dctx); err != nil && !errors.Is(err, context.Canceled) {
			m.log.Error("eventsource: dispatcher exited", "source", sc.Name, "err", err)
		}
	}()
	return nil
}

// stopDispatcher cancels the dispatcher's context, waits for the
// subscriber loop to exit, then waits up to drainTimeout for in-flight
// SubmitInvocation calls to complete before closing the subscriber and
// publisher. On drain timeout the close still proceeds (broker-side
// offsets stay un-advanced for any orphaned in-flight call — see
// repo docs for the at-least-once guarantee semantics).
func (m *Manager) stopDispatcher(name string) error {
	m.mu.Lock()
	entry, ok := m.dispatchers[name]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	delete(m.dispatchers, name)
	m.mu.Unlock()

	entry.cancel()
	<-entry.done

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), drainTimeout)
	defer cancelDrain()
	done := make(chan struct{})
	go func() {
		entry.disp.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-drainCtx.Done():
		m.log.Warn("eventsource: drain timeout; closing subscriber with in-flight submits",
			"source", name, "timeout", drainTimeout)
	}

	var firstErr error
	if err := entry.sub.Close(); err != nil {
		firstErr = err
	}
	if entry.pub != nil {
		if err := entry.pub.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close cancels every dispatcher, waits for graceful drain (per-dispatcher
// up to drainTimeout), then closes subscribers and publishers. Safe to
// call multiple times.
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
	names := make([]string, 0, len(m.dispatchers))
	for n := range m.dispatchers {
		names = append(names, n)
	}
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	var firstErr error
	for _, n := range names {
		if err := m.stopDispatcher(n); err != nil && firstErr == nil {
			firstErr = err
		}
	}
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

// sourceConfigsEqual reports whether two SourceConfigs would build the
// same dispatcher. Used by Reconcile to decide "no change" vs "stop +
// start". reflect.DeepEqual matches the koanf-loaded value shape and is
// the cheapest reliable equality test for this struct (no pointers, no
// time.Time fields that would compare locations).
func sourceConfigsEqual(a, b SourceConfig) bool {
	return reflect.DeepEqual(a, b)
}
