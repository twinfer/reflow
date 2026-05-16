package invoker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Config carries the dependencies the Invoker needs at construction
// time. All fields except Log are required. DeploymentResolver and
// WireDispatcher are optional — when nil, every invocation falls back to
// the in-process registry path. The codec governs wire-payload
// marshalling and defaults to protobuf.
type Config struct {
	Registry        *Registry
	JournalTable    tables.JournalTable
	InvocationTable tables.InvocationTable
	StateTable      tables.StateTable
	Proposer        Proposer
	Log             *slog.Logger

	// Deployments resolves a stamped deployment_id to a DeploymentRecord
	// so installSessionLocked can branch between in-proc dispatch (no
	// record or url scheme == "inproc") and wire dispatch (url scheme ==
	// "http" | "https").
	Deployments DeploymentResolver

	// WireDispatcher opens a remote-handler Stream against a DeploymentRecord.
	// Required when a non-inproc deployment is registered; nil when the
	// host only serves in-process handlers.
	WireDispatcher WireDispatcher

	// Codec governs wire payload encoding (default protobuf).
	Codec handlerclient.Codec
}

// sessionHandle is the union view of *session (inproc) and *wireSession
// (wire) that Invoker tracks per-id. Both types provide the same
// lifecycle methods so the sessions map can hold either.
type sessionHandle interface {
	start()
	abort()
	Done() <-chan struct{}
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
	deployments     DeploymentResolver
	dispatcher      WireDispatcher
	codec           handlerclient.Codec
	log             *slog.Logger

	mu       sync.Mutex
	sessions map[string]sessionHandle
	// pendingRespawn holds StartInvocation requests that arrived while a
	// session was still in the map. The cleanup goroutine drains an entry
	// here after the session exits, ensuring no wake-up is lost when a
	// TimerFired (or other wake) races a session's in-flight Suspended
	// propose. Keyed by sessionKey.
	pendingRespawn map[string]*enginev1.InvocationTarget
	// pendingStart buffers StartInvocation calls that arrived while
	// started=false. Drained by Start. Closes the race where the apply
	// goroutine emits ActInvoke through dispatchActions during the
	// window between Leadership.OnAnnounceLeader flipping IsLeader=true
	// (synchronously) and PartitionRunner.onBecomeLeader's goroutine
	// reaching invoker.Start. Without this buffer, those calls were
	// dropped and the rows stayed Scheduled until external retry.
	pendingStart []pendingStartReq
	ctx          context.Context
	cancel       context.CancelFunc
	started      bool
}

type pendingStartReq struct {
	id     *enginev1.InvocationId
	target *enginev1.InvocationTarget
}

// New constructs an Invoker. The returned value is inactive until Start
// is called.
func New(cfg Config) *Invoker {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	codec := cfg.Codec
	if codec == nil {
		codec = handlerclient.DefaultCodec()
	}
	return &Invoker{
		registry:        cfg.Registry,
		journal:         NewJournalReader(cfg.JournalTable),
		invocationTable: cfg.InvocationTable,
		stateTable:      cfg.StateTable,
		proposer:        cfg.Proposer,
		deployments:     cfg.Deployments,
		dispatcher:      cfg.WireDispatcher,
		codec:           codec,
		log:             log,
		sessions:        make(map[string]sessionHandle),
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
//
// Drains any StartInvocation requests that arrived before Start. Those
// requests come from the apply path: Leadership.OnAnnounceLeader flips
// IsLeader synchronously, after which subsequent apply batches emit
// ActInvoke through dispatchActions. PartitionRunner.onBecomeLeader runs
// in a fresh goroutine and may not have called Start yet by the time the
// apply pump reaches the next batch — without the drain, those wakes
// would be lost.
func (i *Invoker) Start(ctx context.Context) {
	i.mu.Lock()
	if i.cancel != nil {
		i.cancel()
	}
	i.ctx, i.cancel = context.WithCancel(ctx)
	i.started = true
	pending := i.pendingStart
	i.pendingStart = nil
	i.mu.Unlock()

	for _, r := range pending {
		i.StartInvocation(r.id, r.target)
	}
}

// Stop tears down every active session and waits for their goroutines
// to exit. Safe to call multiple times.
func (i *Invoker) Stop() {
	i.mu.Lock()
	cancel := i.cancel
	sessions := i.sessions
	i.sessions = make(map[string]sessionHandle)
	i.pendingRespawn = make(map[string]*enginev1.InvocationTarget)
	i.pendingStart = nil
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
		i.pendingStart = append(i.pendingStart, pendingStartReq{id: id, target: target})
		i.mu.Unlock()
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
// true on success, or (nil, false) when no dispatch path is available
// (no handler AND no wire deployment) or the invoker is no longer
// started. Does NOT call s.start() — the caller drops the lock before
// doing so.
//
// Dispatch selection:
//   - Read the persisted InvocationStatus to retrieve the stamped
//     deployment_id (Phase 5c).
//   - If deployments resolver yields a record whose transport is not
//     "inproc", install a wireSession (requires WireDispatcher).
//   - Otherwise look up the handler in the in-process registry and
//     install the inproc session. A missing handler + missing wire
//     deployment is the only path that drops with a warn — preserving
//     the existing "stays Scheduled" behaviour observed by
//     TestInvokerWiringMissingHandlerStaysScheduled.
func (i *Invoker) installSessionLocked(id *enginev1.InvocationId, target *enginev1.InvocationTarget, key string) (sessionHandle, bool) {
	if !i.started {
		return nil, false
	}

	var rec *enginev1.DeploymentRecord
	if i.deployments != nil {
		status, err := i.invocationTable.Get(id)
		if err != nil {
			i.log.Warn("invoker: load status for dispatch failed",
				"id", invocationIDString(id), "err", err)
			return nil, false
		}
		if depID := status.GetDeploymentId(); depID != "" {
			rec, err = i.deployments.Resolve(depID)
			if err != nil {
				i.log.Warn("invoker: resolve deployment failed",
					"id", invocationIDString(id),
					"deployment_id", depID,
					"err", err)
				// Fall through to in-proc lookup — the deployment may be
				// the synthetic inproc record that the resolver doesn't
				// know about on this node.
			}
		}
	}

	if rec != nil && !isInprocDeployment(rec) {
		if i.dispatcher == nil {
			i.log.Warn("invoker: wire deployment requires WireDispatcher; dropping",
				"id", invocationIDString(id),
				"deployment_id", rec.GetId(),
				"url", rec.GetUrl())
			return nil, false
		}
		s := newWireSession(
			i.ctx,
			id,
			target,
			rec,
			i.dispatcher,
			i.codec,
			i.proposer,
			i.invocationTable,
			i.stateTable,
			i.journal,
			i.log,
		)
		i.sessions[key] = s
		return s, true
	}

	handler, kind, ok := i.registry.Lookup(target)
	if !ok {
		i.log.Warn("invoker: no handler registered; dropping",
			"id", invocationIDString(id),
			"service", target.GetServiceName(),
			"handler", target.GetHandlerName(),
		)
		return nil, false
	}
	// kind rides on the session today purely as metadata; per-kind
	// dispatch (object key locks, workflow lifecycle) lands as kind-
	// aware routing matures.
	// The chanTransport pair is held for legacy in-process plumbing;
	// the in-process Go SDK ignores it and drives the handler directly
	// via *inprocContext. Closing it on session exit is enough.
	engineSide, _ := NewChanTransport()
	s := newSession(
		i.ctx,
		id,
		target,
		handler,
		kind,
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
func (i *Invoker) watchSession(id *enginev1.InvocationId, key string, s sessionHandle) {
	<-s.Done()
	var next sessionHandle
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
// Why this is needed: when this node was a follower for the prior
// leadership scope, the apply path skipped ActInvoke emission (isLeader
// gate in partition.go), so dispatchActions never saw those rows. On
// promotion the new leader must re-spawn sessions for any Scheduled or
// Invoked invocation that committed before its leadership began. The
// pre-Start StartInvocation drain in Start() handles the orthogonal
// goroutine race where dispatchActions runs after Leadership flips but
// before invoker.Start; resume covers the on-disk catch-up case.
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

// AbortInvocation tears down the named session if one exists. Called
// directly from Invoker.Stop on leadership loss; no action type wraps
// this entry point.
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

// isInprocDeployment returns true when rec is the synthetic in-process
// deployment (url scheme == "inproc"). The synthetic record short-circuits
// to the in-process sdk.Registry path; anything else goes through the
// handlerclient wire path.
func isInprocDeployment(rec *enginev1.DeploymentRecord) bool {
	if rec == nil {
		return false
	}
	u := rec.GetUrl()
	const prefix = "inproc://"
	if len(u) < len(prefix) {
		return false
	}
	for i := range len(prefix) {
		c := u[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != prefix[i] {
			return false
		}
	}
	return true
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
