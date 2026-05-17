package invoker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// WireDispatcher resolves a deployment_id to an open handler stream
// over HTTP/2. The invoker constructs one StartMessage per session and
// the dispatcher is the seam where transport configuration lives.
// target carries (service, handler) used to build the URL path.
//
// The single Stream returned by Open is owned by the caller (the
// session goroutine); it must be drained or its parent context
// cancelled to release the underlying HTTP/2 stream.
type WireDispatcher interface {
	Open(ctx context.Context, rec *enginev1.DeploymentRecord, target *enginev1.InvocationTarget) (handlerclient.Stream, error)
}

// wireSession runs one invocation by dispatching protocolv1 frames
// over HTTP/2 to a remote handler deployment. start/abort/Done shape
// the lifecycle; the run loop pumps frames and translates them into
// FSM proposals.
type wireSession struct {
	id     *enginev1.InvocationId
	target *enginev1.InvocationTarget

	// depID + rec are resolved lazily in run() so the apply-path
	// installSessionLocked never blocks on SyncRead. depID is the
	// stamped deployment_id from the InvocationStatus (may be empty
	// for ctx.Call-spawned callees, in which case handlerLookup runs
	// against the (service, handler) index).
	depID         string
	deployments   DeploymentResolver
	handlerLookup HandlerLookup
	rec           *enginev1.DeploymentRecord

	dispatcher WireDispatcher
	codec      handlerclient.Codec
	proposer   Proposer
	invocation tables.InvocationTable
	stateTable tables.StateTable
	journal    *JournalReader
	log        *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	// kind is resolved from rec.Handlers in run() so the
	// StartMessage carries the addressing model the deployment
	// actually advertises for this (service, handler).
	kind protocolv1.Kind

	// nextIdx is the next journal index the engine will assign to an
	// inbound command frame. Slot 0 is reserved for JEInput (written by
	// loadStartInput); state writes start at 1 and increment per
	// frame. Mirrors wireContext.nextSlot on the handler side.
	nextIdx uint32

	// lastRunSlot is the slot id of the most recent RunCommandMessage.
	// handleProposeRunCompletion validates that the following propose
	// targets the same slot; mismatch fails the session.
	lastRunSlot uint32
}

// newWireSession constructs an inactive wire session. Call start to
// spawn its goroutine. depID may be empty — run() falls back to
// handlerLookup against the (service, handler) index. deployments and
// handlerLookup may not both be nil.
func newWireSession(
	parent context.Context,
	id *enginev1.InvocationId,
	target *enginev1.InvocationTarget,
	depID string,
	deployments DeploymentResolver,
	handlerLookup HandlerLookup,
	dispatcher WireDispatcher,
	codec handlerclient.Codec,
	proposer Proposer,
	invocation tables.InvocationTable,
	stateTable tables.StateTable,
	journal *JournalReader,
	log *slog.Logger,
) *wireSession {
	ctx, cancel := context.WithCancel(parent)
	if codec == nil {
		codec = handlerclient.DefaultCodec()
	}
	return &wireSession{
		id:            id,
		target:        target,
		depID:         depID,
		deployments:   deployments,
		handlerLookup: handlerLookup,
		dispatcher:    dispatcher,
		codec:         codec,
		proposer:      proposer,
		invocation:    invocation,
		stateTable:    stateTable,
		journal:       journal,
		log:           log,
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
		nextIdx:       1,
	}
}

func (s *wireSession) start()                { go s.run() }
func (s *wireSession) abort()                { s.cancel() }
func (s *wireSession) Done() <-chan struct{} { return s.done }

func (s *wireSession) run() {
	defer close(s.done)

	entries, ok := s.loadJournal()
	if !ok {
		return
	}

	if !s.resolveDeployment() {
		return
	}

	kind, kindOk := s.resolveKind()
	if !kindOk {
		s.failTerminal(fmt.Sprintf(
			"wire dispatch: deployment %q does not advertise (service=%q, handler=%q)",
			s.rec.GetId(), s.target.GetServiceName(), s.target.GetHandlerName()))
		return
	}
	s.kind = kind

	stream, err := s.dispatcher.Open(s.ctx, s.rec, s.target)
	if err != nil {
		s.log.Warn("invoker.wire: open stream failed",
			"id", invocationIDString(s.id),
			"deployment_id", s.rec.GetId(),
			"url", s.rec.GetUrl(),
			"err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: open: %v", err))
		return
	}
	defer func() { _ = stream.CloseSend() }()

	if err := s.sendStartAndReplay(stream, entries); err != nil {
		if s.ctx.Err() != nil {
			return
		}
		s.log.Warn("invoker.wire: send Start+replay failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: send start: %v", err))
		return
	}

	// nextIdx picks up after the highest existing slot — any new
	// command frame the handler emits gets the next sequential index.
	s.nextIdx = highestIndex(entries) + 1

	s.driveLoop(stream)
}

// loadJournal reads the invocation status, advances Scheduled to
// Invoked by proposing JEInput when needed, and returns the full journal
// (in index order) for replay. Returns ok=false when the session should
// bail (e.g. already Completed, status load failed, context cancelled).
func (s *wireSession) loadJournal() ([]*enginev1.JournalEntry, bool) {
	status, err := s.invocation.Get(s.id)
	if err != nil {
		s.log.Warn("invoker.wire: load status failed",
			"id", invocationIDString(s.id), "err", err)
		return nil, false
	}
	switch st := status.GetStatus().(type) {
	case nil, *enginev1.InvocationStatus_Free, *enginev1.InvocationStatus_Completed:
		return nil, false
	case *enginev1.InvocationStatus_Scheduled:
		entry := &enginev1.JournalEntry{
			Index: 0,
			Entry: &enginev1.JournalEntry_Input{
				Input: &enginev1.JEInput{Value: st.Scheduled.GetInput()},
			},
		}
		if err := s.proposeJournal(entry); err != nil {
			if !errors.Is(err, context.Canceled) && s.ctx.Err() == nil {
				s.log.Warn("invoker.wire: propose JEInput failed",
					"id", invocationIDString(s.id), "err", err)
			}
			return nil, false
		}
		// Scheduled → just-Invoked path: the only journal entry is the
		// JEInput we just proposed. Hand-craft it rather than reload
		// from Pebble (the journal table read isn't read-your-writes
		// coherent with the propose path).
		return []*enginev1.JournalEntry{entry}, true
	case *enginev1.InvocationStatus_Invoked, *enginev1.InvocationStatus_Suspended:
		// Already past Scheduled: pull the full journal for replay.
		entries, err := s.journal.Load(s.id)
		if err != nil {
			s.log.Warn("invoker.wire: load journal failed",
				"id", invocationIDString(s.id), "err", err)
			return nil, false
		}
		return entries, true
	default:
		return nil, false
	}
}

// highestIndex returns the largest Index across the supplied journal
// entries, or 0 if the slice is empty. The +1 in run() turns this into
// "next free slot."
func highestIndex(entries []*enginev1.JournalEntry) uint32 {
	var highest uint32
	for _, e := range entries {
		if idx := e.GetIndex(); idx > highest {
			highest = idx
		}
	}
	return highest
}

// sendStartAndReplay emits the StartMessage frame followed by one
// frame per existing journal entry (translated by wire_replay.go).
// Handlers count frames received and transition to user-code phase
// when received == known_entries.
//
// StartMessage.state_map carries the eager-preloaded K/V snapshot for
// the invocation's (service, object_key) so wireContext.GetState can
// serve hits directly from the handler-side cache without a round-trip.
func (s *wireSession) sendStartAndReplay(stream handlerclient.Stream, entries []*enginev1.JournalEntry) error {
	frames, err := translateJournal(s.id, entries, s.codec, s.log)
	if err != nil {
		return fmt.Errorf("translate journal: %w", err)
	}

	start := &protocolv1.StartMessage{
		Id:           s.id.GetUuid(),
		DebugId:      invocationIDString(s.id),
		Key:          s.target.GetObjectKey(),
		ServiceName:  s.target.GetServiceName(),
		HandlerName:  s.target.GetHandlerName(),
		KnownEntries: uint32(len(frames)),
		PartitionKey: s.id.GetPartitionKey(),
	}
	start.Kind = s.kind
	cache, overflowed := preloadEagerState(s.stateTable, s.target, s.id, s.log)
	if len(cache) > 0 {
		stateEntries := make([]*protocolv1.StartMessage_StateEntry, 0, len(cache))
		for k, v := range cache {
			stateEntries = append(stateEntries, &protocolv1.StartMessage_StateEntry{
				Key:   []byte(k),
				Value: v,
			})
		}
		start.StateMap = stateEntries
	}
	// PartialState=true means the snapshot is incomplete; handler
	// errors on cache miss instead of treating the missing key as
	// absent. Set when preload overflowed eagerStateMaxBytes.
	start.PartialState = overflowed
	startBytes, err := s.codec.Marshal(start)
	if err != nil {
		return fmt.Errorf("marshal StartMessage: %w", err)
	}
	if err := stream.Send(handlerclient.FrameFor(handlerclient.TypeStart, startBytes)); err != nil {
		return err
	}
	for _, f := range frames {
		if err := stream.Send(handlerclient.FrameFor(f.typeCode, f.payload)); err != nil {
			return err
		}
	}
	return nil
}

// resolveDeployment populates s.rec by resolving s.depID against the
// DeploymentResolver. When s.depID is empty (ctx.Call-spawned callees
// arrive without a stamped deployment_id), the (service, handler)
// index is consulted via s.handlerLookup. Returns false and fails the
// session terminally when no deployment can be found.
func (s *wireSession) resolveDeployment() bool {
	if s.rec != nil {
		return true
	}
	depID := s.depID
	if depID == "" {
		if s.handlerLookup == nil {
			s.log.Warn("invoker.wire: missing deployment_id with no handler lookup; dropping",
				"id", invocationIDString(s.id),
				"service", s.target.GetServiceName(),
				"handler", s.target.GetHandlerName())
			s.failTerminal(fmt.Sprintf(
				"wire dispatch: no deployment registered for %s/%s",
				s.target.GetServiceName(), s.target.GetHandlerName()))
			return false
		}
		resolved, err := s.handlerLookup(s.ctx, s.target.GetServiceName(), s.target.GetHandlerName())
		if err != nil {
			if errors.Is(err, context.Canceled) || s.ctx.Err() != nil {
				return false
			}
			s.log.Warn("invoker.wire: handler lookup failed",
				"id", invocationIDString(s.id), "err", err)
			s.failTerminal(fmt.Sprintf("wire dispatch: handler lookup: %v", err))
			return false
		}
		if resolved == "" {
			s.log.Warn("invoker.wire: no deployment registered for handler; dropping",
				"id", invocationIDString(s.id),
				"service", s.target.GetServiceName(),
				"handler", s.target.GetHandlerName())
			s.failTerminal(fmt.Sprintf(
				"wire dispatch: no deployment registered for %s/%s",
				s.target.GetServiceName(), s.target.GetHandlerName()))
			return false
		}
		depID = resolved
	}
	rec, err := s.deployments.Resolve(s.ctx, depID)
	if err != nil {
		if errors.Is(err, context.Canceled) || s.ctx.Err() != nil {
			return false
		}
		s.log.Warn("invoker.wire: resolve deployment failed",
			"id", invocationIDString(s.id),
			"deployment_id", depID, "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: resolve deployment: %v", err))
		return false
	}
	if rec == nil {
		s.log.Warn("invoker.wire: deployment not found; dropping",
			"id", invocationIDString(s.id),
			"deployment_id", depID)
		s.failTerminal(fmt.Sprintf("wire dispatch: deployment %q not found", depID))
		return false
	}
	s.depID = depID
	s.rec = rec
	return true
}

// resolveKind looks up the protocolv1.Kind the deployment advertises
// for this invocation's (service, handler) tuple. Returns ok=false when
// no exact match exists — the caller fails the invocation terminally
// rather than silently dispatching with a guessed addressing model.
func (s *wireSession) resolveKind() (protocolv1.Kind, bool) {
	for _, h := range s.rec.GetHandlers() {
		if h.GetService() == s.target.GetServiceName() && h.GetHandler() == s.target.GetHandlerName() {
			return protocolv1.Kind(h.GetKind()), true
		}
	}
	return protocolv1.Kind_KIND_UNSPECIFIED, false
}

// driveLoop is the inbound-frame pump. Translates each protocolv1 frame
// into the matching journal append or terminal effect, exiting when the
// handler signals End / Error / Suspension or the stream breaks.
func (s *wireSession) driveLoop(stream handlerclient.Stream) {
	for {
		if s.ctx.Err() != nil {
			return
		}
		f, err := stream.Recv()
		if err == io.EOF {
			// Handler closed without sending EndMessage; treat as
			// premature end. Propose failure so the invocation
			// terminates rather than dangling.
			s.failTerminal("wire dispatch: handler stream closed without EndMessage")
			return
		}
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			s.log.Warn("invoker.wire: stream Recv failed",
				"id", invocationIDString(s.id), "err", err)
			s.failTerminal(fmt.Sprintf("wire dispatch: recv: %v", err))
			return
		}
		if err := handlerclient.ValidatePayload(f); err != nil {
			s.failTerminal(err.Error())
			return
		}
		typeCode, _, _ := handlerclient.UnpackHeader(f.GetHeader())
		switch typeCode {
		case handlerclient.TypeCmdOutput:
			if !s.handleOutput(f.GetPayload()) {
				return
			}
		case handlerclient.TypeEnd:
			// EndMessage carries no payload; the session is done.
			return
		case handlerclient.TypeError:
			s.handleError(f.GetPayload())
			return
		case handlerclient.TypeCmdSetState:
			if !s.handleSetState(f.GetPayload()) {
				return
			}
		case handlerclient.TypeCmdClearState:
			if !s.handleClearState(f.GetPayload()) {
				return
			}
		case handlerclient.TypeCmdClearAllState:
			if !s.handleClearAllState(f.GetPayload()) {
				return
			}
		case handlerclient.TypeCmdSleep:
			if !s.handleSleep(f.GetPayload()) {
				return
			}
		case handlerclient.TypeCmdCall:
			if !s.handleCall(f.GetPayload()) {
				return
			}
		case handlerclient.TypeCmdOneWayCall:
			if !s.handleOneWayCall(f.GetPayload()) {
				return
			}
		case handlerclient.TypeCmdRun:
			// Marker frame: record the SDK-stated slot. The actual
			// JERun proposal arrives via ProposeRunCompletionMessage.
			// We pin nextIdx to result_completion_id+1 so retry
			// markers (which re-use the same slot as the first
			// attempt) don't cause our cursor to drift past the
			// handler's wireContext.nextSlot.
			if !s.handleRunMarker(f.GetPayload()) {
				return
			}
		case handlerclient.TypeProposeRunDone:
			if !s.handleProposeRunCompletion(f.GetPayload()) {
				return
			}
		case handlerclient.TypeCmdAwakeable:
			if !s.handleAwakeable(f.GetPayload()) {
				return
			}
		case handlerclient.TypeSuspension:
			s.handleSuspension(f.GetPayload())
			return
		default:
			// Forward-compat: unknown type codes are logged and skipped.
			// The handler may emit frames the engine hasn't taught itself
			// to parse yet; tolerating them keeps a newer SDK working
			// against an older engine for additive features.
			s.log.Warn("invoker.wire: unhandled frame type",
				"id", invocationIDString(s.id), "type", fmt.Sprintf("0x%04x", typeCode))
		}
	}
}

// handleOutput decodes an OutputCommandMessage and proposes the
// matching InvokerEffect.Completed. Returns false on failure so the
// caller exits the loop.
func (s *wireSession) handleOutput(payload []byte) bool {
	var out protocolv1.OutputCommandMessage
	if err := s.codec.Unmarshal(payload, &out); err != nil {
		s.log.Warn("invoker.wire: decode OutputCommandMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode output: %v", err))
		return false
	}
	var (
		output     []byte
		failureMsg string
	)
	switch r := out.GetResult().(type) {
	case *protocolv1.OutputCommandMessage_Value:
		output = r.Value.GetContent()
	case *protocolv1.OutputCommandMessage_Failure:
		failureMsg = r.Failure.GetMessage()
	}
	s.completeTerminal(output, failureMsg)
	return false
}

// handleError translates an ErrorMessage into a terminal failure. The
// SDK-supplied code is preserved in the failure message string for
// observability; structured codes will be threaded once the invocation
// FSM grows a failure_code field.
func (s *wireSession) handleError(payload []byte) {
	var em protocolv1.ErrorMessage
	if err := s.codec.Unmarshal(payload, &em); err != nil {
		s.log.Warn("invoker.wire: decode ErrorMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode error: %v", err))
		return
	}
	msg := em.GetMessage()
	if em.GetCode() != 0 {
		msg = fmt.Sprintf("[%d] %s", em.GetCode(), msg)
	}
	s.failTerminal(msg)
}

// handleSetState decodes a SetStateCommandMessage and proposes the
// matching JESetState journal entry. Returns false to bail the driveLoop
// on a decode or propose failure.
func (s *wireSession) handleSetState(payload []byte) bool {
	var cmd protocolv1.SetStateCommandMessage
	if err := s.codec.Unmarshal(payload, &cmd); err != nil {
		s.log.Warn("invoker.wire: decode SetStateCommandMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode set_state: %v", err))
		return false
	}
	entry := &enginev1.JournalEntry{
		Index: s.allocIdx(),
		Entry: &enginev1.JournalEntry_SetState{
			SetState: &enginev1.JESetState{
				Key:   string(cmd.GetKey()),
				Value: cmd.GetValue().GetContent(),
			},
		},
	}
	return s.proposeJournalOrFail(entry, "JESetState")
}

// handleClearState decodes a ClearStateCommandMessage and proposes the
// matching JEClearState entry.
func (s *wireSession) handleClearState(payload []byte) bool {
	var cmd protocolv1.ClearStateCommandMessage
	if err := s.codec.Unmarshal(payload, &cmd); err != nil {
		s.log.Warn("invoker.wire: decode ClearStateCommandMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode clear_state: %v", err))
		return false
	}
	entry := &enginev1.JournalEntry{
		Index: s.allocIdx(),
		Entry: &enginev1.JournalEntry_ClearState{
			ClearState: &enginev1.JEClearState{Key: string(cmd.GetKey())},
		},
	}
	return s.proposeJournalOrFail(entry, "JEClearState")
}

// handleClearAllState decodes a ClearAllStateCommandMessage and
// proposes JEClearAllState.
func (s *wireSession) handleClearAllState(payload []byte) bool {
	var cmd protocolv1.ClearAllStateCommandMessage
	if err := s.codec.Unmarshal(payload, &cmd); err != nil {
		s.log.Warn("invoker.wire: decode ClearAllStateCommandMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode clear_all_state: %v", err))
		return false
	}
	entry := &enginev1.JournalEntry{
		Index: s.allocIdx(),
		Entry: &enginev1.JournalEntry_ClearAllState{
			ClearAllState: &enginev1.JEClearAllState{},
		},
	}
	return s.proposeJournalOrFail(entry, "JEClearAllState")
}

// handleSleep decodes a SleepCommandMessage and proposes JESleep at
// the next free slot. The handler allocated 2 slots (cmd + result);
// the engine consumes the result slot too so its nextIdx stays in
// sync with the handler's wireContext.nextSlot. The result slot itself
// is written later by the FSM when the timer fires (JESleepResult).
func (s *wireSession) handleSleep(payload []byte) bool {
	var cmd protocolv1.SleepCommandMessage
	if err := s.codec.Unmarshal(payload, &cmd); err != nil {
		s.log.Warn("invoker.wire: decode SleepCommandMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode sleep: %v", err))
		return false
	}
	cmdIdx := s.allocIdx()
	// Reserve the result slot so subsequent commands skip past it.
	_ = s.allocIdx()
	entry := &enginev1.JournalEntry{
		Index: cmdIdx,
		Entry: &enginev1.JournalEntry_Sleep{
			Sleep: &enginev1.JESleep{FireAtMs: cmd.GetWakeUpTime()},
		},
	}
	return s.proposeJournalOrFail(entry, "JESleep")
}

// handleCall decodes a CallCommandMessage and proposes the matching
// JECall journal entry. Allocates 2 slots (cmd + result) to keep its
// nextIdx in sync with the handler's wireContext, which reserves the
// same pair.
func (s *wireSession) handleCall(payload []byte) bool {
	var cmd protocolv1.CallCommandMessage
	if err := s.codec.Unmarshal(payload, &cmd); err != nil {
		s.log.Warn("invoker.wire: decode CallCommandMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode call: %v", err))
		return false
	}
	cmdIdx := s.allocIdx()
	_ = s.allocIdx() // reserve result slot
	entry := &enginev1.JournalEntry{
		Index: cmdIdx,
		Entry: &enginev1.JournalEntry_Call{
			Call: &enginev1.JECall{
				Target: &enginev1.InvocationTarget{
					ServiceName: cmd.GetServiceName(),
					HandlerName: cmd.GetHandlerName(),
					ObjectKey:   cmd.GetKey(),
				},
				Input:          cmd.GetParameter(),
				IdempotencyKey: cmd.GetIdempotencyToken(),
			},
		},
	}
	return s.proposeJournalOrFail(entry, "JECall")
}

// handleOneWayCall decodes an OneWayCallCommandMessage and proposes
// JEOneWayCall. Single-slot — fire-and-forget, no result-pair.
func (s *wireSession) handleOneWayCall(payload []byte) bool {
	var cmd protocolv1.OneWayCallCommandMessage
	if err := s.codec.Unmarshal(payload, &cmd); err != nil {
		s.log.Warn("invoker.wire: decode OneWayCallCommandMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode one_way_call: %v", err))
		return false
	}
	entry := &enginev1.JournalEntry{
		Index: s.allocIdx(),
		Entry: &enginev1.JournalEntry_OneWayCall{
			OneWayCall: &enginev1.JEOneWayCall{
				Target: &enginev1.InvocationTarget{
					ServiceName: cmd.GetServiceName(),
					HandlerName: cmd.GetHandlerName(),
					ObjectKey:   cmd.GetKey(),
				},
				Input:          cmd.GetParameter(),
				IdempotencyKey: cmd.GetIdempotencyToken(),
			},
		},
	}
	return s.proposeJournalOrFail(entry, "JEOneWayCall")
}

// handleAwakeable decodes an AwakeableCommandMessage and proposes
// JEAwakeable carrying the SDK-minted id. Allocates 2 slots (cmd +
// result) so nextIdx stays in sync with the handler's wireContext.
func (s *wireSession) handleAwakeable(payload []byte) bool {
	var cmd protocolv1.AwakeableCommandMessage
	if err := s.codec.Unmarshal(payload, &cmd); err != nil {
		s.log.Warn("invoker.wire: decode AwakeableCommandMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode awakeable: %v", err))
		return false
	}
	cmdIdx := s.allocIdx()
	_ = s.allocIdx() // reserve result slot
	entry := &enginev1.JournalEntry{
		Index: cmdIdx,
		Entry: &enginev1.JournalEntry_Awakeable{
			Awakeable: &enginev1.JEAwakeable{AwakeableId: cmd.GetAwakeableId()},
		},
	}
	return s.proposeJournalOrFail(entry, "JEAwakeable")
}

// handleRunMarker decodes a RunCommandMessage and pins nextIdx to
// result_completion_id+1. Unlike Sleep/Call/Awakeable which use
// allocIdx, Run reuses the same slot across retry attempts — the
// handler emits another marker at the same slot on each respawn that
// re-runs fn, so we trust the SDK's slot id rather than blindly
// advancing.
func (s *wireSession) handleRunMarker(payload []byte) bool {
	var cmd protocolv1.RunCommandMessage
	if err := s.codec.Unmarshal(payload, &cmd); err != nil {
		s.log.Warn("invoker.wire: decode RunCommandMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode run: %v", err))
		return false
	}
	s.lastRunSlot = cmd.GetResultCompletionId()
	if next := s.lastRunSlot + 1; next > s.nextIdx {
		s.nextIdx = next
	}
	return true
}

// handleProposeRunCompletion decodes a ProposeRunCompletionMessage and
// proposes the matching InvokerEffect_RunProposal. The FSM apply path
// writes JERun with entry_index=result_completion_id carrying the
// (value | failure_message, retryable) outcome.
//
// Validates that result_completion_id matches the slot of the
// preceding RunCommandMessage; the pair travels together and a
// mismatch indicates a buggy SDK.
func (s *wireSession) handleProposeRunCompletion(payload []byte) bool {
	var prop protocolv1.ProposeRunCompletionMessage
	if err := s.codec.Unmarshal(payload, &prop); err != nil {
		s.log.Warn("invoker.wire: decode ProposeRunCompletionMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode propose_run: %v", err))
		return false
	}
	if prop.GetResultCompletionId() != s.lastRunSlot {
		s.failTerminal(fmt.Sprintf(
			"wire dispatch: ProposeRunCompletion result_completion_id=%d does not match preceding RunCommand slot=%d",
			prop.GetResultCompletionId(), s.lastRunSlot))
		return false
	}
	rp := &enginev1.JERunProposal{
		EntryIndex:  prop.GetResultCompletionId(),
		Retryable:   prop.GetRetryable(),
		RetryPolicy: wireRetryPolicy(prop.GetRetryPolicy()),
	}
	switch r := prop.GetResult().(type) {
	case *protocolv1.ProposeRunCompletionMessage_Value:
		rp.Value = r.Value
	case *protocolv1.ProposeRunCompletionMessage_Failure:
		rp.FailureMessage = r.Failure.GetMessage()
	}
	eff := &enginev1.InvokerEffect{
		InvocationId: s.id,
		Kind:         &enginev1.InvokerEffect_RunProposal{RunProposal: rp},
	}
	if err := s.proposeEffect(eff); err != nil {
		if errors.Is(err, context.Canceled) || s.ctx.Err() != nil {
			return false
		}
		s.log.Warn("invoker.wire: propose RunProposal failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: propose run: %v", err))
		return false
	}
	return true
}

// wireRetryPolicy translates the protocolv1 retry policy onto its
// enginev1 twin. Returns nil for a nil/zero input so the apply path
// uses its defaults.
func wireRetryPolicy(p *protocolv1.RunRetryPolicy) *enginev1.RunRetryPolicy {
	if p == nil {
		return nil
	}
	if p.GetInitialIntervalMs() == 0 && p.GetFactor() == 0 &&
		p.GetMaxIntervalMs() == 0 && p.GetMaxAttempts() == 0 {
		return nil
	}
	return &enginev1.RunRetryPolicy{
		InitialIntervalMs: p.GetInitialIntervalMs(),
		Factor:            p.GetFactor(),
		MaxIntervalMs:     p.GetMaxIntervalMs(),
		MaxAttempts:       p.GetMaxAttempts(),
	}
}

// handleSuspension translates a SuspensionMessage into
// InvokerEffect_Suspended and terminates the session. The
// `awaiting_on` field on InvocationSuspended is descriptive — the
// engine's wake path (Suspended → Invoked + ActInvoke on the next
// completion event) doesn't consult its contents. We synthesize
// human-readable labels from the wire's uint32 completion ids for
// observability.
func (s *wireSession) handleSuspension(payload []byte) {
	if s.ctx.Err() != nil {
		return
	}
	var sm protocolv1.SuspensionMessage
	if err := s.codec.Unmarshal(payload, &sm); err != nil {
		s.log.Warn("invoker.wire: decode SuspensionMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode suspension: %v", err))
		return
	}
	awaiting := make([]string, 0,
		len(sm.GetWaitingCompletions())+
			len(sm.GetWaitingSignals())+
			len(sm.GetWaitingNamedSignals()))
	for _, id := range sm.GetWaitingCompletions() {
		awaiting = append(awaiting, fmt.Sprintf("completion:%d", id))
	}
	for _, id := range sm.GetWaitingSignals() {
		awaiting = append(awaiting, fmt.Sprintf("signal:%d", id))
	}
	awaiting = append(awaiting, sm.GetWaitingNamedSignals()...)

	eff := &enginev1.InvokerEffect{
		InvocationId: s.id,
		Kind: &enginev1.InvokerEffect_Suspended{
			Suspended: &enginev1.InvocationSuspended{
				AwaitingOn: awaiting,
			},
		},
	}
	if err := s.proposeEffect(eff); err != nil && s.ctx.Err() == nil {
		s.log.Warn("invoker.wire: propose Suspended failed",
			"id", invocationIDString(s.id), "err", err)
	}
}

// allocIdx reserves the next journal index for an engine-assigned
// command. Single-threaded by virtue of driveLoop's serial frame pump.
func (s *wireSession) allocIdx() uint32 {
	idx := s.nextIdx
	s.nextIdx++
	return idx
}

// proposeJournalOrFail wraps proposeJournal with the failTerminal
// pattern used across the inbound frame handlers. kind is the
// human-readable JE name for log diagnostics.
func (s *wireSession) proposeJournalOrFail(entry *enginev1.JournalEntry, kind string) bool {
	if err := s.proposeJournal(entry); err != nil {
		if errors.Is(err, context.Canceled) || s.ctx.Err() != nil {
			return false
		}
		s.log.Warn("invoker.wire: propose "+kind+" failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: propose %s: %v", kind, err))
		return false
	}
	return true
}

// completeTerminal proposes Completed for this invocation.
func (s *wireSession) completeTerminal(output []byte, failureMsg string) {
	if s.ctx.Err() != nil {
		return
	}
	eff := &enginev1.InvokerEffect{
		InvocationId: s.id,
		Kind: &enginev1.InvokerEffect_Completed{
			Completed: &enginev1.InvocationCompleted{
				Output:         output,
				FailureMessage: failureMsg,
			},
		},
	}
	if err := s.proposeEffect(eff); err != nil && s.ctx.Err() == nil {
		s.log.Warn("invoker.wire: propose Completed failed",
			"id", invocationIDString(s.id), "err", err)
	}
}

func (s *wireSession) failTerminal(msg string) {
	s.completeTerminal(nil, msg)
}

func (s *wireSession) proposeJournal(entry *enginev1.JournalEntry) error {
	eff := &enginev1.InvokerEffect{
		InvocationId: s.id,
		Kind: &enginev1.InvokerEffect_JournalAppended{
			JournalAppended: &enginev1.JournalEntryAppended{Entry: entry},
		},
	}
	return s.proposeEffect(eff)
}

func (s *wireSession) proposeEffect(eff *enginev1.InvokerEffect) error {
	cmd := &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: eff}}
	propCtx, cancel := context.WithTimeout(s.ctx, proposeTimeout)
	defer cancel()
	return s.proposer.ProposeSelf(propCtx, cmd)
}
