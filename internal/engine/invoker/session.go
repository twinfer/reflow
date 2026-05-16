package invoker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twinfer/reflow/internal/storage/tables"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Proposer is the subset of engine.RaftProposer the session uses to
// commit its journal appends. Carved out so unit tests can substitute a
// fake without dragging dragonboat into the package.
type Proposer interface {
	ProposeSelf(ctx context.Context, cmd *enginev1.Command) error
}

// proposeTimeout bounds a single ProposeSelf call. Mirrors the value the
// timer service uses (timer_service.go:288). Independent of session.ctx
// so a stuck Raft doesn't hang the handler indefinitely.
const proposeTimeout = 5 * time.Second

// sessionState mirrors restate's InvocationStateMachine states
// (invocation_state_machine.rs:35-63). Transitions are linear:
// Initial → Replay → Bidi → (Suspended | Terminated).
type sessionState int

const (
	sessInitial sessionState = iota
	sessReplay
	sessBidi
	sessSuspended
	sessTerminated
)

func (s sessionState) String() string {
	switch s {
	case sessInitial:
		return "Initial"
	case sessReplay:
		return "Replay"
	case sessBidi:
		return "Bidi"
	case sessSuspended:
		return "Suspended"
	case sessTerminated:
		return "Terminated"
	default:
		return "Unknown"
	}
}

// session represents a single running invocation. Owned by the Invoker;
// each session runs the protocol state machine on its own goroutine.
type session struct {
	id      *enginev1.InvocationId
	target  *enginev1.InvocationTarget
	handler sdk.Handler
	kind    sdk.Kind

	proposer   Proposer
	journal    *JournalReader
	invocation tables.InvocationTable
	stateTable tables.StateTable
	log        *slog.Logger

	transport SessionTransport // engine side of the pair

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu    sync.Mutex
	state sessionState
}

// newSession constructs an inactive session. Call start to spawn its
// goroutine.
func newSession(
	parent context.Context,
	id *enginev1.InvocationId,
	target *enginev1.InvocationTarget,
	handler sdk.Handler,
	kind sdk.Kind,
	proposer Proposer,
	journal *JournalReader,
	invocation tables.InvocationTable,
	state tables.StateTable,
	transport SessionTransport,
	log *slog.Logger,
) *session {
	ctx, cancel := context.WithCancel(parent)
	return &session{
		id:         id,
		target:     target,
		handler:    handler,
		kind:       kind,
		proposer:   proposer,
		journal:    journal,
		invocation: invocation,
		stateTable: state,
		log:        log,
		transport:  transport,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
		state:      sessInitial,
	}
}

// start kicks off the session goroutine.
func (s *session) start() {
	go s.run()
}

// abort tells the session to tear down. Idempotent; safe to call from
// any goroutine. Callers should also drain Done().
func (s *session) abort() {
	s.cancel()
}

// Done returns a channel closed when the session goroutine has exited.
func (s *session) Done() <-chan struct{} { return s.done }

// setState atomically transitions to next, logging the previous→next
// move. Transition validity is not enforced here — Restate's checks
// live one layer up in the SDK side.
func (s *session) setState(next sessionState) {
	s.mu.Lock()
	prev := s.state
	s.state = next
	s.mu.Unlock()
	if s.log != nil {
		s.log.Debug("invoker.session: state",
			"id", invocationIDString(s.id),
			"from", prev.String(),
			"to", next.String(),
		)
	}
}

// run is the per-session goroutine. Drives:
//
//	Initial → Replay      — read status, propose JEInput if Scheduled, load journal
//	Replay  → Bidi        — spawn handler goroutine, await its return
//	Bidi    → Suspended   — handler returned sdk.ErrSuspended; propose Suspended
//	Bidi    → Terminated  — handler returned (value | *sdk.Failure); propose Completed
//
// On context cancellation at any point the goroutine exits without
// proposing — the in-flight invocation stays Invoked and the next leader
// resumes from the journal.
func (s *session) run() {
	defer close(s.done)
	defer func() {
		if s.transport != nil {
			_ = s.transport.Close()
		}
	}()

	input, journalIndex, stateCache, ok := s.prepare()
	if !ok {
		return
	}

	s.setState(sessBidi)
	ictx := newInprocContext(s, input, journalIndex, stateCache)
	output, handlerErr := s.runHandler(ictx)

	s.publishOutcome(ictx, output, handlerErr)
}

// eagerStateMaxBytes caps the eager state preload at session start. When
// the total payload across all rows for an object exceeds this, the
// preload is dropped and GetState falls back to its existing
// not-implemented path. 64 KiB matches the plan's documented limit.
const eagerStateMaxBytes = 64 * 1024

// prepare runs the Replay phase. Returns the input bytes, the journal
// index map, the eager state cache (nil when overflowed or not applicable),
// and ok=true if the session should proceed to Bidi. A false ok indicates
// the caller should exit (already completed, status load failed, or
// shutdown in progress).
func (s *session) prepare() ([]byte, map[uint32]*enginev1.JournalEntry, map[string][]byte, bool) {
	s.setState(sessReplay)

	// Log the SDK build id at session start so operators can correlate
	// behavior changes with SDK upgrades. The value is cached by
	// sdk.BuildID, so the cost is one map lookup per session.
	if bid := sdk.BuildID(); bid != "" {
		s.log.Info("invoker.session: start",
			"id", invocationIDString(s.id),
			"sdk_build_id", bid,
		)
	}

	status, err := s.invocation.Get(s.id)
	if err != nil {
		s.log.Warn("invoker.session: load status failed",
			"id", invocationIDString(s.id), "err", err)
		return nil, nil, nil, false
	}

	switch st := status.GetStatus().(type) {
	case nil, *enginev1.InvocationStatus_Free, *enginev1.InvocationStatus_Completed:
		// Nothing to run.
		return nil, nil, nil, false
	case *enginev1.InvocationStatus_Scheduled:
		// First execution: propose JEInput at index 0 to flip
		// Scheduled → Invoked. SyncPropose blocks until the apply
		// commits, so on return the journal is durable.
		inputEntry := &enginev1.JournalEntry{
			Index: 0,
			Entry: &enginev1.JournalEntry_Input{
				Input: &enginev1.JEInput{Value: st.Scheduled.GetInput()},
			},
		}
		if err := s.proposeJournal(inputEntry); err != nil {
			if !errors.Is(err, context.Canceled) && s.ctx.Err() == nil {
				s.log.Warn("invoker.session: propose JEInput failed",
					"id", invocationIDString(s.id), "err", err)
			}
			return nil, nil, nil, false
		}
	case *enginev1.InvocationStatus_Invoked, *enginev1.InvocationStatus_Suspended:
		// Re-run after suspension or restart. Journal already has JEInput.
	}

	entries, err := s.journal.Load(s.id)
	if err != nil {
		s.log.Warn("invoker.session: load journal failed",
			"id", invocationIDString(s.id), "err", err)
		return nil, nil, nil, false
	}

	journalIndex := make(map[uint32]*enginev1.JournalEntry, len(entries))
	var input []byte
	for _, e := range entries {
		journalIndex[e.GetIndex()] = e
		if e.GetIndex() == 0 {
			if ie, ok := e.GetEntry().(*enginev1.JournalEntry_Input); ok {
				input = ie.Input.GetValue()
			}
		}
	}

	stateCache := s.preloadState()
	return input, journalIndex, stateCache, true
}

// preloadState eagerly reads every state row scoped to this invocation's
// (service, object_key) into an in-memory map served to GetState.
// Thin wrapper around preloadEagerState; inproc cares only about the
// cache (overflow → nil → GetState returns errNotImplemented).
func (s *session) preloadState() map[string][]byte {
	cache, _ := preloadEagerState(s.stateTable, s.target, s.id, s.log)
	return cache
}

// preloadEagerState reads every state row scoped to (service, object_key)
// into an in-memory map. Returns (nil, false) when the scan completed
// for an unkeyed service or failed mid-scan; (nil, true) when the total
// payload exceeded eagerStateMaxBytes — wire callers set
// StartMessage.PartialState in that case so the handler errors on a
// cache miss rather than treating the (incomplete) snapshot as
// authoritative.
//
// Shared by inproc *session and wireSession so both impls present the
// same eager-state surface to handlers.
func preloadEagerState(
	stateTable tables.StateTable,
	target *enginev1.InvocationTarget,
	id *enginev1.InvocationId,
	log *slog.Logger,
) (cache map[string][]byte, overflowed bool) {
	if target.GetObjectKey() == "" {
		return nil, false
	}
	cache = make(map[string][]byte)
	total := 0
	err := stateTable.ScanObject(target, func(key string, value []byte) error {
		total += len(key) + len(value)
		if total > eagerStateMaxBytes {
			overflowed = true
			return errStatePreloadOverflow
		}
		cache[key] = append([]byte(nil), value...)
		return nil
	})
	if overflowed {
		log.Info("invoker.session: state preload overflow; falling back to lazy",
			"id", invocationIDString(id),
			"limit_bytes", eagerStateMaxBytes,
		)
		return nil, true
	}
	if err != nil {
		log.Warn("invoker.session: state preload scan failed",
			"id", invocationIDString(id), "err", err)
		return nil, false
	}
	return cache, false
}

// errStatePreloadOverflow is the sentinel returned from ScanObject's
// callback to short-circuit a too-large scan.
var errStatePreloadOverflow = errors.New("state preload overflow")

// runHandler invokes the registered handler on its own goroutine and
// waits for it to return. Ctx cancellation is observed so abort()
// doesn't have to wait for a wedged handler.
func (s *session) runHandler(ictx *inprocContext) ([]byte, error) {
	type result struct {
		out []byte
		err error
	}
	ch := make(chan result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{err: fmt.Errorf("handler panic: %v", r)}
			}
		}()
		out, err := s.handler(ictx, ictx.input)
		ch <- result{out: out, err: err}
	}()

	select {
	case r := <-ch:
		return r.out, r.err
	case <-s.ctx.Done():
		// Handler is still running; we leak its goroutine until it
		// observes ictx.Context() being cancelled. That happens because
		// s.ctx is what ictx.Context() returns.
		return nil, s.ctx.Err()
	}
}

// publishOutcome translates the handler's return value into a terminal
// InvokerEffect and proposes it. Three buckets:
//
//	context.Canceled / s.ctx already done   → exit (no propose)
//	sdk.ErrSuspended                       → propose Suspended{awaiting_on}
//	nil | *sdk.Failure | other err         → propose Completed{output|failure}
//
// On a transient error from a ctx method (e.g. propose hit a network glitch
// and returned ProposeSelf's error to the handler) we still mark the
// invocation Completed with the error text. The alternative — bailing
// silently — leaves the invocation Invoked with no path forward, since
// no future event re-fires ActInvoke. Surfacing the error as a terminal
// Completed failure favours visibility over silent stalls.
func (s *session) publishOutcome(ictx *inprocContext, output []byte, handlerErr error) {
	if s.ctx.Err() != nil || errors.Is(handlerErr, context.Canceled) {
		return
	}

	if errors.Is(handlerErr, sdk.ErrSuspended) {
		s.setState(sessSuspended)
		ictx.mu.Lock()
		awaiting := append([]string(nil), ictx.suspendedOn...)
		ictx.mu.Unlock()
		eff := &enginev1.InvokerEffect{
			InvocationId: s.id,
			Kind: &enginev1.InvokerEffect_Suspended{
				Suspended: &enginev1.InvocationSuspended{AwaitingOn: awaiting},
			},
		}
		if pErr := s.proposeEffect(eff); pErr != nil && s.ctx.Err() == nil {
			s.log.Warn("invoker.session: propose Suspended failed",
				"id", invocationIDString(s.id), "err", pErr)
		}
		return
	}

	var failureMsg string
	if f, ok := sdk.AsFailure(handlerErr); ok {
		failureMsg = f.Message
	} else if handlerErr != nil {
		failureMsg = handlerErr.Error()
	}

	s.setState(sessTerminated)
	eff := &enginev1.InvokerEffect{
		InvocationId: s.id,
		Kind: &enginev1.InvokerEffect_Completed{
			Completed: &enginev1.InvocationCompleted{
				Output:         output,
				FailureMessage: failureMsg,
			},
		},
	}
	if pErr := s.proposeEffect(eff); pErr != nil && s.ctx.Err() == nil {
		s.log.Warn("invoker.session: propose Completed failed",
			"id", invocationIDString(s.id), "err", pErr)
	}
}

// proposeJournal wraps entry in an InvokerEffect.JournalAppended and
// submits it via ProposeSelf. Blocks until commit or proposeTimeout.
func (s *session) proposeJournal(entry *enginev1.JournalEntry) error {
	eff := &enginev1.InvokerEffect{
		InvocationId: s.id,
		Kind: &enginev1.InvokerEffect_JournalAppended{
			JournalAppended: &enginev1.JournalEntryAppended{Entry: entry},
		},
	}
	return s.proposeEffect(eff)
}

// proposeEffect submits an InvokerEffect through the proposer. A bounded
// deadline keeps a stuck Raft from hanging the handler goroutine; the
// session ctx is honoured for shutdown.
func (s *session) proposeEffect(eff *enginev1.InvokerEffect) error {
	cmd := &enginev1.Command{
		Kind: &enginev1.Command_InvokerEffect{InvokerEffect: eff},
	}
	propCtx, cancel := context.WithTimeout(s.ctx, proposeTimeout)
	defer cancel()
	return s.proposer.ProposeSelf(propCtx, cmd)
}

// sessionKey builds a stable string key from id's raw 24-byte
// representation (8-byte partition_key BE || 16-byte uuid). Used as the
// map key in Invoker.sessions; cheaper than reflect.DeepEqual and stable
// across replays.
func sessionKey(id *enginev1.InvocationId) string {
	if id == nil {
		return ""
	}
	var buf [24]byte
	binary.BigEndian.PutUint64(buf[:8], id.GetPartitionKey())
	copy(buf[8:24], id.GetUuid())
	return string(buf[:])
}

// invocationIDString renders id as "<partition_key_hex>:<uuid_hex>" for
// log lines. Lazy-allocated — only called inside log-statements.
func invocationIDString(id *enginev1.InvocationId) string {
	if id == nil {
		return "<nil>"
	}
	var pk [8]byte
	binary.BigEndian.PutUint64(pk[:], id.GetPartitionKey())
	return hex(pk[:]) + ":" + hex(id.GetUuid())
}

func hex(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, x := range b {
		out[i*2] = digits[x>>4]
		out[i*2+1] = digits[x&0x0f]
	}
	return string(out)
}
