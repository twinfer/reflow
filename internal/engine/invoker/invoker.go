package invoker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Config carries the dependencies the Invoker needs at construction
// time. All fields except Log are required.
type Config struct {
	Registry        *Registry
	JournalTable    tables.JournalTable
	InvocationTable tables.InvocationTable
	StateTable      tables.StateTable
	Proposer        Proposer
	Log             *slog.Logger
}

// Invoker owns the in-process invocation sessions for one partition.
// Constructed at host startup, activated on leader-gain via Start, and
// torn down on leader-loss via Stop. The PartitionRunner forwards
// Actions to the per-action methods below.
//
// Thread-safety: Start/Stop are called by the runner's leadership
// callbacks (single-threaded with respect to each other). The per-action
// methods are called from the apply-path dispatch loop, also
// single-threaded. The internal mutex protects against concurrent
// abort/lookup operations.
type Invoker struct {
	registry        *Registry
	journal         *JournalReader
	invocationTable tables.InvocationTable
	stateTable      tables.StateTable
	proposer        Proposer
	log             *slog.Logger

	mu       sync.Mutex
	sessions map[string]*session
	// pendingRespawn holds StartInvocation requests that arrived while a
	// session was still in the map. The cleanup goroutine drains an entry
	// here after the session exits, ensuring no wake-up is lost when a
	// TimerFired (or other wake) races a session's in-flight Suspended
	// propose. Keyed by sessionKey.
	pendingRespawn map[string]*enginev1.InvocationTarget
	ctx            context.Context
	cancel         context.CancelFunc
	started        bool
}

// New constructs an Invoker. The returned value is inactive until Start
// is called.
func New(cfg Config) *Invoker {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	return &Invoker{
		registry:        cfg.Registry,
		journal:         NewJournalReader(cfg.JournalTable),
		invocationTable: cfg.InvocationTable,
		stateTable:      cfg.StateTable,
		proposer:        cfg.Proposer,
		log:             log,
		sessions:        make(map[string]*session),
		pendingRespawn:  make(map[string]*enginev1.InvocationTarget),
	}
}

// Rebind swaps the underlying storage handles after a snapshot recovery
// has replaced the Pebble DB on disk. Mirrors timer/outbox rebind
// patterns elsewhere in the engine package.
func (i *Invoker) Rebind(journal tables.JournalTable, invocations tables.InvocationTable, state tables.StateTable) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.journal.Rebind(journal)
	i.invocationTable = invocations
	i.stateTable = state
}

// Start activates the Invoker. Calling Start a second time without an
// intervening Stop is a programming error but does not panic — the
// previous context is replaced.
func (i *Invoker) Start(ctx context.Context) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.cancel != nil {
		i.cancel()
	}
	i.ctx, i.cancel = context.WithCancel(ctx)
	i.started = true
}

// Stop tears down every active session and waits for their goroutines
// to exit. Safe to call multiple times.
func (i *Invoker) Stop() {
	i.mu.Lock()
	cancel := i.cancel
	sessions := i.sessions
	i.sessions = make(map[string]*session)
	i.pendingRespawn = make(map[string]*enginev1.InvocationTarget)
	i.cancel = nil
	i.started = false
	i.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, s := range sessions {
		s.abort()
		<-s.Done()
	}
}

// StartInvocation spawns a session for the given id. The spawn is
// idempotent against a still-running session — the existing goroutine
// picks up new journal entries on its own loop iteration. When the
// previous session has already exited (suspended itself, terminated
// normally, or aborted), the slot is reclaimed and a fresh session
// goroutine is started.
func (i *Invoker) StartInvocation(id *enginev1.InvocationId, target *enginev1.InvocationTarget) {
	i.mu.Lock()
	if !i.started {
		i.mu.Unlock()
		i.log.Warn("invoker: StartInvocation before Start; dropping",
			"id", invocationIDString(id))
		return
	}
	key := sessionKey(id)
	if existing, ok := i.sessions[key]; ok {
		// A session is recorded for this key. If it has already
		// finished (e.g. proposed Suspended and returned), reclaim the
		// slot and spawn a fresh one — that is the Suspended → Invoked
		// wake-up path. Otherwise record a pending respawn so the
		// cleanup goroutine spawns a fresh session as soon as the
		// current one exits — this is what makes a TimerFired (or any
		// wake) racing with the session's in-flight Suspended propose
		// safe: the wake's ActInvoke would otherwise be swallowed by
		// the "session already in map" check and never re-issue.
		select {
		case <-existing.Done():
			delete(i.sessions, key)
		default:
			i.pendingRespawn[key] = target
			i.mu.Unlock()
			return
		}
	}
	s, ok := i.installSessionLocked(id, target, key)
	i.mu.Unlock()
	if !ok {
		return
	}
	s.start()
	go i.watchSession(id, key, s)
}

// installSessionLocked constructs the session and installs it in the
// sessions map. MUST be called with i.mu held; returns the session and
// true on success, or (nil, false) when no handler is registered or the
// invoker is no longer started. Does NOT call s.start() — the caller
// drops the lock before doing so.
func (i *Invoker) installSessionLocked(id *enginev1.InvocationId, target *enginev1.InvocationTarget, key string) (*session, bool) {
	if !i.started {
		return nil, false
	}
	handler, ok := i.registry.Lookup(target)
	if !ok {
		i.log.Warn("invoker: no handler registered; dropping",
			"id", invocationIDString(id),
			"service", target.GetServiceName(),
			"handler", target.GetHandlerName(),
		)
		return nil, false
	}
	// The chanTransport pair is held for the wire-shim path (Step 15);
	// the in-process Go SDK ignores it and drives the handler directly
	// via *invocationContext. Closing it on session exit is enough.
	engineSide, _ := NewChanTransport()
	s := newSession(
		i.ctx,
		id,
		target,
		handler,
		i.proposer,
		i.journal,
		i.invocationTable,
		i.stateTable,
		engineSide,
		i.log,
	)
	i.sessions[key] = s
	return s, true
}

// watchSession runs in its own goroutine for the lifetime of one
// session. Cleans up the sessions map entry on exit, and if a wake
// arrived while the session was running, installs a fresh session for
// the pending target — all under one lock acquisition so a concurrent
// StartInvocation cannot install a duplicate session.
func (i *Invoker) watchSession(id *enginev1.InvocationId, key string, s *session) {
	<-s.Done()
	var next *session
	i.mu.Lock()
	if cur, ok := i.sessions[key]; ok && cur == s {
		delete(i.sessions, key)
	}
	respawnTarget, queued := i.pendingRespawn[key]
	if queued {
		delete(i.pendingRespawn, key)
		next, _ = i.installSessionLocked(id, respawnTarget, key)
	}
	i.mu.Unlock()
	if next != nil {
		next.start()
		go i.watchSession(id, key, next)
	}
}

// ResumeNonTerminal spawns sessions for every persisted Scheduled or
// Invoked invocation. Called from PartitionRunner.onBecomeLeader after
// Start.
//
// Why this is needed: ActInvoke is emitted from FSM transitions inside
// applyCommand. On a single-node partition, replay-on-startup re-applies
// committed Raft log entries BEFORE leadership election completes, and
// the apply path's dispatchActions calls StartInvocation while the
// Invoker is not yet started — those calls are dropped with a warning.
// Without this resume step, any Scheduled/Invoked invocation that
// committed before the previous Host closed would never get a session
// on the new leader.
//
// Suspended invocations are intentionally NOT resumed here: they will
// be woken by their pending wake-event (TimerFired / AwakeableResolved
// / SignalDelivered / CallResult journal append), which re-emits
// ActInvoke through the normal apply-path dispatch. Spawning a session
// proactively races with that wake-event — if the resumed session is
// still in the sessions map when the wake-event's ActInvoke arrives,
// StartInvocation sees a live entry and silently no-ops, losing the
// wake. The wake-event path is sufficient on its own because the
// TimerService rebuilds from disk and re-fires past-due timers, and
// the awakeable/signal/call-result paths come from external proposals
// that happen after leader gain.
func (i *Invoker) ResumeNonTerminal(table tables.InvocationTable) error {
	i.mu.Lock()
	if !i.started {
		i.mu.Unlock()
		return nil
	}
	i.mu.Unlock()

	return table.ScanAll(func(id *enginev1.InvocationId, s *enginev1.InvocationStatus) error {
		var target *enginev1.InvocationTarget
		switch st := s.GetStatus().(type) {
		case *enginev1.InvocationStatus_Scheduled:
			target = st.Scheduled.GetTarget()
		case *enginev1.InvocationStatus_Invoked:
			target = st.Invoked.GetTarget()
		default:
			return nil
		}
		if target == nil {
			i.log.Warn("invoker: resume skip; no target on status",
				"id", invocationIDString(id),
				"status", fmt.Sprintf("%T", s.GetStatus()))
			return nil
		}
		i.StartInvocation(id, target)
		return nil
	})
}

// AbortInvocation tears down the named session if one exists. Used on
// leadership loss (the runner emits ActAbortInvocation per active id)
// and on explicit Cancel.
func (i *Invoker) AbortInvocation(id *enginev1.InvocationId) {
	key := sessionKey(id)
	i.mu.Lock()
	s, ok := i.sessions[key]
	if ok {
		delete(i.sessions, key)
	}
	i.mu.Unlock()
	if ok {
		s.abort()
		<-s.Done()
	}
}

// DeliverNotification forwards a Completion to the named session. Step
// 11 wires it to the session's notification mailbox; Step 10 only
// validates that a session exists.
func (i *Invoker) DeliverNotification(
	id *enginev1.InvocationId,
	completionID uint32,
	value []byte,
	failure string,
	void bool,
) {
	i.mu.Lock()
	_, ok := i.sessions[sessionKey(id)]
	i.mu.Unlock()
	if !ok {
		// Suspended session — the next StartInvocation will observe the
		// completion in the journal on replay.
		return
	}
	_ = completionID
	_ = value
	_ = failure
	_ = void
	// Step 11: route to session's notification channel.
}

// DeliverAwakeable surfaces an external awakeable resolution to a
// running session's in-memory state, so a poll observes it without a
// journal re-read. Step 11 implementation.
func (i *Invoker) DeliverAwakeable(
	id *enginev1.InvocationId,
	awakeableID string,
	value []byte,
	failure string,
) {
	i.mu.Lock()
	_, ok := i.sessions[sessionKey(id)]
	i.mu.Unlock()
	if !ok {
		return
	}
	_ = awakeableID
	_ = value
	_ = failure
	// Step 11: route to session's awakeable map.
}

// activeSessions returns a snapshot of currently-active session keys.
// Tests use this to assert lifecycle correctness.
func (i *Invoker) activeSessions() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make([]string, 0, len(i.sessions))
	for k := range i.sessions {
		out = append(out, k)
	}
	return out
}
