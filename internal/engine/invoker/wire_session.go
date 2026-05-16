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

// WireDispatcher resolves a deployment_id to an open handler stream.
// The invoker constructs one StartMessage per session and the dispatcher
// is the seam where transport selection (gRPC / HTTP/2) lives. target
// carries (service, handler) — HTTP/2 uses it to build the URL path;
// gRPC ignores it because the routing rides on StartMessage.
//
// The single Stream returned by Open is owned by the caller (the
// session goroutine); it must be drained or its parent context
// cancelled to release the underlying gRPC stream.
type WireDispatcher interface {
	Open(ctx context.Context, rec *enginev1.DeploymentRecord, target *enginev1.InvocationTarget) (handlerclient.Stream, error)
}

// wireSession runs one invocation by dispatching over the wire to a
// remote handler. It mirrors *session's lifecycle (start/abort/Done)
// but the run loop drives protocolv1 frames instead of an in-process
// handler goroutine.
//
// 5d.1 implements the minimum protocol: send StartMessage, await a
// single OutputCommandMessage + EndMessage from the handler, propose
// Completed. State/sleep/call/awakeable land as the wire-session matures.
type wireSession struct {
	id     *enginev1.InvocationId
	target *enginev1.InvocationTarget
	rec    *enginev1.DeploymentRecord

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

	// nextIdx is the next journal index the engine will assign to an
	// inbound command frame. Slot 0 is reserved for JEInput (written by
	// loadStartInput); state writes start at 1 and increment per
	// frame. Mirrors wireContext.nextSlot on the handler side.
	nextIdx uint32
}

// newWireSession constructs an inactive wire session. Call start to
// spawn its goroutine.
func newWireSession(
	parent context.Context,
	id *enginev1.InvocationId,
	target *enginev1.InvocationTarget,
	rec *enginev1.DeploymentRecord,
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
		id:         id,
		target:     target,
		rec:        rec,
		dispatcher: dispatcher,
		codec:      codec,
		proposer:   proposer,
		invocation: invocation,
		stateTable: stateTable,
		journal:    journal,
		log:        log,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
		nextIdx:    1,
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
	var max uint32
	for _, e := range entries {
		if idx := e.GetIndex(); idx > max {
			max = idx
		}
	}
	return max
}

// sendStartAndReplay emits the StartMessage frame followed by one
// frame per existing journal entry (translated by wire_replay.go).
// Handlers count frames received and transition to user-code phase
// when received == known_entries.
//
// StartMessage.state_map carries the eager-preloaded K/V snapshot for
// the invocation's (service, object_key) so wireContext.GetState can
// serve hits directly from the handler-side cache without a round-trip.
// Mirrors inproc.go's preloadState semantics.
func (s *wireSession) sendStartAndReplay(stream handlerclient.Stream, entries []*enginev1.JournalEntry) error {
	frames, err := translateJournal(entries, s.codec, s.log)
	if err != nil {
		return fmt.Errorf("translate journal: %w", err)
	}

	start := &protocolv1.StartMessage{
		Id:           s.id.GetUuid(),
		DebugId:      invocationIDString(s.id),
		Key:          s.target.GetObjectKey(),
		Kind:         s.kindForTarget(),
		ServiceName:  s.target.GetServiceName(),
		HandlerName:  s.target.GetHandlerName(),
		KnownEntries: uint32(len(frames)),
	}
	if cache := preloadEagerState(s.stateTable, s.target, s.id, s.log); len(cache) > 0 {
		stateEntries := make([]*protocolv1.StartMessage_StateEntry, 0, len(cache))
		for k, v := range cache {
			stateEntries = append(stateEntries, &protocolv1.StartMessage_StateEntry{
				Key:   []byte(k),
				Value: v,
			})
		}
		start.StateMap = stateEntries
	}
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

// kindForTarget picks the protocolv1.Kind that matches this
// invocation's (service, handler) tuple from the deployment record.
// Defaults to KIND_SERVICE when the record has no exact match — the
// handler will reject if the addressing model disagrees, which beats
// silently dispatching with an arbitrary kind.
func (s *wireSession) kindForTarget() protocolv1.Kind {
	for _, h := range s.rec.GetHandlers() {
		if h.GetService() == s.target.GetServiceName() && h.GetHandler() == s.target.GetHandlerName() {
			return protocolv1.Kind(h.GetKind())
		}
	}
	return protocolv1.Kind_KIND_SERVICE
}

// driveLoop is the inbound-frame pump. 5d.1 handles only the minimum:
// OutputCommandMessage + EndMessage → propose Completed. ErrorMessage
// also terminates the session with a failure.
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
			// Marker frame: advance slot counter so the next
			// engine-assigned index lines up with handler's wireContext.
			// The actual JERun proposal arrives via
			// ProposeRunCompletionMessage below.
			_ = s.allocIdx()
		case handlerclient.TypeProposeRunDone:
			if !s.handleProposeRunCompletion(f.GetPayload()) {
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

// handleProposeRunCompletion decodes a ProposeRunCompletionMessage and
// proposes the matching InvokerEffect_RunProposal. The FSM apply path
// writes JERun with entry_index=result_completion_id carrying the
// (value | failure_message, retryable) outcome.
func (s *wireSession) handleProposeRunCompletion(payload []byte) bool {
	var prop protocolv1.ProposeRunCompletionMessage
	if err := s.codec.Unmarshal(payload, &prop); err != nil {
		s.log.Warn("invoker.wire: decode ProposeRunCompletionMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: decode propose_run: %v", err))
		return false
	}
	rp := &enginev1.JERunProposal{
		EntryIndex: prop.GetResultCompletionId(),
		Retryable:  prop.GetRetryable(),
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

// completeTerminal proposes Completed for this invocation. Same shape
// as the inproc session's terminal propose so the FSM treats both
// dispatch paths uniformly.
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
