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
	journal    *JournalReader
	log        *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
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
		journal:    journal,
		log:        log,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
}

func (s *wireSession) start()                { go s.run() }
func (s *wireSession) abort()                { s.cancel() }
func (s *wireSession) Done() <-chan struct{} { return s.done }

func (s *wireSession) run() {
	defer close(s.done)

	input, ok := s.loadStartInput()
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

	if err := s.sendStart(stream, input); err != nil {
		if s.ctx.Err() != nil {
			return
		}
		s.log.Warn("invoker.wire: send StartMessage failed",
			"id", invocationIDString(s.id), "err", err)
		s.failTerminal(fmt.Sprintf("wire dispatch: send start: %v", err))
		return
	}

	s.driveLoop(stream)
}

// loadStartInput reads the invocation status, advances Scheduled to
// Invoked by proposing JEInput when needed, and returns the input bytes
// for StartMessage. Returns ok=false when the session should bail (e.g.
// already Completed, status load failed, context cancelled).
func (s *wireSession) loadStartInput() ([]byte, bool) {
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
		return st.Scheduled.GetInput(), true
	case *enginev1.InvocationStatus_Invoked, *enginev1.InvocationStatus_Suspended:
		// Already past Scheduled: pull input from the journal's index 0.
		entries, err := s.journal.Load(s.id)
		if err != nil {
			s.log.Warn("invoker.wire: load journal failed",
				"id", invocationIDString(s.id), "err", err)
			return nil, false
		}
		for _, e := range entries {
			if e.GetIndex() == 0 {
				if ie, ok := e.GetEntry().(*enginev1.JournalEntry_Input); ok {
					return ie.Input.GetValue(), true
				}
			}
		}
		return nil, true // journal load empty; handler may still proceed
	default:
		return nil, false
	}
}

// sendStart codec-encodes and emits the StartMessage frame.
func (s *wireSession) sendStart(stream handlerclient.Stream, input []byte) error {
	start := &protocolv1.StartMessage{
		Id:      s.id.GetUuid(),
		DebugId: invocationIDString(s.id),
		Key:     s.target.GetObjectKey(),
		Kind:    s.kindForTarget(),
		// known_entries / state_map / partial_state stay zero in the
		// minimal wire path; state preload and InputCommandMessage
		// replay land as the wire-session matures.
	}
	_ = input // input rides on the journal (JEInput); future commits replay it as InputCommandMessage.
	payload, err := s.codec.Marshal(start)
	if err != nil {
		return fmt.Errorf("marshal StartMessage: %w", err)
	}
	return stream.Send(handlerclient.FrameFor(handlerclient.TypeStart, payload))
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
