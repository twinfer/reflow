package invoker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/internal/storage/tables"
	"github.com/twinfer/reflw/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// Config carries the dependencies the Invoker needs at construction
// time. Deployments + WireDispatcher are required for any invocation to
// run; with either nil, every StartInvocation drops the row with a warn.
// Codec governs wire-payload marshalling and defaults to protobuf.
type Config struct {
	JournalTable    tables.JournalTable
	InvocationTable tables.InvocationTable
	StateTable      tables.StateTable
	Proposer        Proposer
	Log             *slog.Logger

	// Deployments resolves a stamped deployment_id to a DeploymentRecord
	// so installSessionLocked can open a wire session against the
	// deployment's URL.
	Deployments DeploymentResolver

	// HandlerLookup resolves (service, handler) → deployment_id against
	// shard 0's deployment index. Used as a fallback when an invocation
	// arrives without deployment_id stamped — happens today for
	// ctx.Call-spawned callees because the JECall apply arm has no shard-0
	// view. Returns "" + nil when no deployment claims the handler.
	HandlerLookup HandlerLookup

	// WireDispatcher opens a remote-handler Stream against a DeploymentRecord.
	WireDispatcher WireDispatcher

	// Codec governs wire payload encoding (default protobuf).
	Codec wire.Codec

	// EagerStateMaxBytes caps the eager-state snapshot delivered with
	// StartMessage. Zero means "use DefaultEagerStateMaxBytes" (64 KiB).
	// Operators tune via Config.Handlers.EagerStateMaxBytes.
	EagerStateMaxBytes uint32

	// ProcessEngine runs reflwos process/case turns in-process. Nil disables
	// process execution (StartProcessTurn drops with a warn). Injected so the
	// engine package never imports reflwos.
	ProcessEngine ProcessEngine

	// ProcessInstanceTable / ProcessInboxTable back the process-execution path;
	// rebound on leader gain like the other table views.
	ProcessInstanceTable tables.ProcessInstanceTable
	ProcessInboxTable    tables.ProcessInboxTable
}

// HandlerLookup resolves (service, handler) → deployment_id.
type HandlerLookup func(ctx context.Context, service, handler string) (string, error)

// sessionHandle is the lifecycle surface every concrete session
// implementation exposes. Today only *wireSession implements it.
type sessionHandle interface {
	start()
	abort()
	Done() <-chan struct{}
}

// Invoker owns the per-partition invocation sessions. Constructed at
// host startup, activated on leader-gain via Start, and torn down on
// leader-loss via Stop. The PartitionRunner forwards Actions to the
// per-action methods below.
//
// Thread-safety: Start/Stop are called by the runner's leadership
// callbacks (single-threaded with respect to each other). The per-action
// methods are called from the apply-path dispatch loop, also
// single-threaded. The internal mutex protects against concurrent
// abort/lookup operations.
type Invoker struct {
	journal            *JournalReader
	invocationTable    tables.InvocationTable
	stateTable         tables.StateTable
	proposer           Proposer
	deployments        DeploymentResolver
	handlerLookup      HandlerLookup
	dispatcher         WireDispatcher
	codec              wire.Codec
	eagerStateMaxBytes uint32
	log                *slog.Logger

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

	// Process-execution path. procSessions tracks in-flight process turns
	// (one per instance, keyed by procSessionKey); processEngine is nil when
	// no reflwos binding is injected.
	processEngine    ProcessEngine
	processInstances tables.ProcessInstanceTable
	processInbox     tables.ProcessInboxTable
	procSessions     map[string]sessionHandle
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
		codec = wire.DefaultCodec()
	}
	return &Invoker{
		journal:            NewJournalReader(cfg.JournalTable),
		invocationTable:    cfg.InvocationTable,
		stateTable:         cfg.StateTable,
		proposer:           cfg.Proposer,
		deployments:        cfg.Deployments,
		handlerLookup:      cfg.HandlerLookup,
		dispatcher:         cfg.WireDispatcher,
		codec:              codec,
		eagerStateMaxBytes: cfg.EagerStateMaxBytes,
		log:                log,
		sessions:           make(map[string]sessionHandle),
		pendingRespawn:     make(map[string]*enginev1.InvocationTarget),
		procSessions:       make(map[string]sessionHandle),
		processEngine:      cfg.ProcessEngine,
		processInstances:   cfg.ProcessInstanceTable,
		processInbox:       cfg.ProcessInboxTable,
	}
}

// Rebind swaps the underlying storage handles after a snapshot recovery
// has replaced the Pebble DB on disk. Mirrors timer/outbox rebind
// patterns elsewhere in the engine package.
func (i *Invoker) Rebind(journal tables.JournalTable, invocations tables.InvocationTable, state tables.StateTable, procInstances tables.ProcessInstanceTable, procInbox tables.ProcessInboxTable) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.journal.Rebind(journal)
	i.invocationTable = invocations
	i.stateTable = state
	i.processInstances = procInstances
	i.processInbox = procInbox
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
	procSessions := i.procSessions
	i.sessions = make(map[string]sessionHandle)
	i.procSessions = make(map[string]sessionHandle)
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
	for _, s := range procSessions {
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

// installSessionLocked constructs the wire session and installs it in
// the sessions map. MUST be called with i.mu held; returns the session
// and true on success, or (nil, false) when no deployment is registered
// for the invocation's (service, handler), the deployment lookup fails,
// or the invoker is no longer started. Does NOT call s.start() — the
// caller drops the lock before doing so.
func (i *Invoker) installSessionLocked(id *enginev1.InvocationId, target *enginev1.InvocationTarget, key string) (sessionHandle, bool) {
	if !i.started {
		return nil, false
	}
	if i.deployments == nil || i.dispatcher == nil {
		i.log.Warn("invoker: dispatch dependencies missing; dropping",
			"id", invocationIDString(id),
			"has_deployments", i.deployments != nil,
			"has_dispatcher", i.dispatcher != nil)
		return nil, false
	}

	status, err := i.invocationTable.Get(id)
	if err != nil {
		i.log.Warn("invoker: load status for dispatch failed",
			"id", invocationIDString(id), "err", err)
		return nil, false
	}

	// The deployment_id resolution and DeploymentRecord lookup happen
	// inside wireSession.run() (in its own goroutine) rather than here:
	// installSessionLocked is on the apply-path dispatch loop, and a
	// blocking SyncRead would stall apply → propose deadlock.
	s := newWireSession(
		i.ctx,
		id,
		target,
		status.GetDeploymentId(),
		i.deployments,
		i.handlerLookup,
		i.dispatcher,
		i.codec,
		i.proposer,
		i.invocationTable,
		i.stateTable,
		i.journal,
		i.eagerStateMaxBytes,
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
//
// Caller (onBecomeLeader) must hold a Snapshotter lease for the table's
// backing store; the lease guarantees the underlying pebble.DB stays
// open for the duration of the scan, so this function never panics with
// pebble.ErrClosed.
//
// ctx is the leader-scope context. ResumeNonTerminal checks ctx.Err()
// between rows so a step-down mid-scan exits promptly without spawning
// invoker sessions whose leader is already gone — cooperative shutdown
// on top of the lease's correctness guarantee.
func (i *Invoker) ResumeNonTerminal(ctx context.Context, table tables.InvocationTable) error {
	i.mu.Lock()
	if !i.started {
		i.mu.Unlock()
		return nil
	}
	i.mu.Unlock()

	return table.ScanAll(ctx, func(id *enginev1.InvocationId, s *enginev1.InvocationStatus) error {
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

// StartProcessTurn spawns a one-shot processSession for the instance: run the
// reflwos engine on event and propose ProcessAdvanced. The apply path serializes
// turns per instance via the process inbox, so a session never overlaps another
// for the same instance under normal operation; a lingering session (e.g. a
// resume racing a fresh activation) is detected and the duplicate dropped.
// Turns that arrive before Start are dropped — the active inbox seq is durable,
// so ResumeProcessTurns re-drives them on leader gain.
func (i *Invoker) StartProcessTurn(pk uint64, service, instanceKey string, entry *enginev1.ProcessInboxEntry) {
	i.mu.Lock()
	if !i.started {
		i.mu.Unlock()
		return
	}
	if i.processEngine == nil {
		i.mu.Unlock()
		i.log.Warn("invoker: ActAdvanceProcess with no process engine; dropping",
			"service", service, "key", instanceKey)
		return
	}
	key := procSessionKey(pk, service, instanceKey)
	if existing, ok := i.procSessions[key]; ok {
		select {
		case <-existing.Done():
			delete(i.procSessions, key)
		default:
			// The prior turn's session is still being reclaimed. The apply-path
			// cursor only emits the activation for inbox seq N+1 AFTER seq N's
			// ProcessAdvanced applied, so this is a genuinely new turn the running
			// (seq N) session will NOT cover — dropping it strands the seq (lost
			// wakeup: a parallel/MI join whose final feedback turn never runs
			// freezes the instance at active_seq>0). Re-drive from the durable
			// record once the predecessor finishes; redriveActiveTurn reads the
			// current active seq, so a redundant re-drive (the resume-vs-fresh
			// race this guard originally caught) is a safe no-op, not a re-apply.
			i.mu.Unlock()
			go func() {
				select {
				case <-existing.Done():
					i.redriveActiveTurn(pk, service, instanceKey)
				case <-i.ctx.Done():
				}
			}()
			return
		}
	}
	ref := processRef{pk: pk, service: service, instanceKey: instanceKey}
	s := newProcessSession(i.ctx, ref, entry, i.processEngine, i.processInstances, i.proposer, i.log)
	i.procSessions[key] = s
	i.mu.Unlock()
	s.start()
	go i.watchProcessSession(key, s)
}

// watchProcessSession reclaims the procSessions slot when a turn finishes.
// Unlike invocation sessions there is no respawn here: the next turn is driven
// by the apply path (ProcessAdvanced activates the next inbox seq, which
// re-emits ActAdvanceProcess), not by the invoker.
func (i *Invoker) watchProcessSession(key string, s sessionHandle) {
	<-s.Done()
	i.mu.Lock()
	if cur, ok := i.procSessions[key]; ok && cur == s {
		delete(i.procSessions, key)
	}
	i.mu.Unlock()
}

// redriveActiveTurn re-issues a process turn for the instance's CURRENT durable
// active seq, if it still has one. It reads the record rather than re-using a
// caller-supplied entry, so it always drives the seq the apply path most
// recently activated — making a redundant or no-longer-needed call a safe no-op
// (absorbed by onProcessAdvanced, which keys off the record's active seq). Used
// by StartProcessTurn to re-drive an activation that landed while the prior
// turn's session was still being reclaimed, instead of dropping it.
func (i *Invoker) redriveActiveTurn(pk uint64, service, instanceKey string) {
	lp := keys.LPFromPartitionKey(pk)
	rec, ok, err := i.processInstances.Get(lp, service, instanceKey)
	if err != nil || !ok || rec.GetActiveSeq() == 0 ||
		rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
		return
	}
	entry, ok, err := i.processInbox.Get(lp, service, instanceKey, rec.GetActiveSeq())
	if err != nil || !ok {
		return
	}
	i.StartProcessTurn(pk, service, instanceKey, entry)
}

// ResumeProcessTurns re-drives any instance whose inbox has an active turn.
// Called from onBecomeLeader after Start, mirroring ResumeNonTerminal: an
// ActAdvanceProcess that committed before this leader scope was dropped (the
// invoker was not yet started), so the new leader must re-issue it. The active
// seq is durable on ProcessInstanceRecord and Advance is pure w.r.t.
// (record, event, logical_time), so re-driving reproduces the same turn.
func (i *Invoker) ResumeProcessTurns(ctx context.Context, instances tables.ProcessInstanceTable, inbox tables.ProcessInboxTable) error {
	i.mu.Lock()
	started := i.started
	engine := i.processEngine
	i.mu.Unlock()
	if !started || engine == nil {
		return nil
	}
	return instances.ScanAll(ctx, func(service, instanceKey string, rec *enginev1.ProcessInstanceRecord) error {
		if rec.GetActiveSeq() == 0 || rec.GetStatus() != enginev1.ProcessStatus_PROCESS_STATUS_RUNNING {
			return nil
		}
		pk := rec.GetRootId().GetPartitionKey()
		lp := keys.LPFromPartitionKey(pk)
		entry, ok, err := inbox.Get(lp, service, instanceKey, rec.GetActiveSeq())
		if err != nil {
			i.log.Warn("invoker: resume process turn; inbox load failed",
				"service", service, "key", instanceKey, "err", err)
			return nil
		}
		if !ok {
			i.log.Warn("invoker: resume process turn; active inbox row missing",
				"service", service, "key", instanceKey, "active_seq", rec.GetActiveSeq())
			return nil
		}
		i.StartProcessTurn(pk, service, instanceKey, entry)
		return nil
	})
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
