package invoker

import (
	"context"
	"log/slog"

	"github.com/twinfer/reflow/internal/storage/keys"
	"github.com/twinfer/reflow/internal/storage/tables"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// ProcessEngine runs one deterministic step of an iflow process/case instance.
// It is injected (the iflow binding lives outside internal/engine — in
// pkg/reflow or an iflow-side adapter) so the engine never imports iflow. This
// is the same dependency inversion WireDispatcher/InProcDialer use to keep
// handler code out of the engine.
//
// Advance must be pure and deterministic with respect to (Record, Event,
// LogicalTimeMs): the implementation reaches wall-clock time only through
// LogicalTimeMs (fed to the iflow engine clock), so a turn re-driven on a new
// leader reproduces the same ProcessAdvanced byte-for-byte. Advance does no
// external I/O; a model/evaluation failure of THIS instance is returned as an
// error and the session converts it into a failed ProcessTerminal.
type ProcessEngine interface {
	Advance(ctx context.Context, in ProcessAdvanceInput) (*enginev1.ProcessAdvanced, error)
}

// ProcessAdvanceInput is one turn's input: the instance's pinned record
// (model_ref + kind + current state_blob) and the turn payload (Entry: an
// opaque iflow event or reflow-native feedback, plus the stamped logical time
// the engine clock reads). Pk/Service/InstanceKey are echoed onto the returned
// ProcessAdvanced so the apply path can address the instance row.
type ProcessAdvanceInput struct {
	Pk          uint64
	Service     string
	InstanceKey string
	Record      *enginev1.ProcessInstanceRecord
	Entry       *enginev1.ProcessInboxEntry
}

// processRef addresses one process instance.
type processRef struct {
	pk          uint64
	service     string
	instanceKey string
}

// processSession runs exactly one instance turn: load the record, hand it to
// the ProcessEngine with the stamped clock, and propose the resulting
// ProcessAdvanced. Unlike wireSession there is no interactive protocol — the
// exchange is a single Advance call — and no per-instance overlap to guard
// against: the apply path serializes turns through the process inbox,
// activating the next queued event only after this turn's ProcessAdvanced
// commits.
type processSession struct {
	ref       processRef
	entry     *enginev1.ProcessInboxEntry
	engine    ProcessEngine
	instances tables.ProcessInstanceTable
	proposer  Proposer
	log       *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

var _ sessionHandle = (*processSession)(nil)

// newProcessSession constructs an inactive session. Call start to spawn its
// goroutine. entry is the turn payload (opaque iflow event or reflow-native
// feedback) plus the stamped logical instant the ProcessEngine feeds the clock.
func newProcessSession(
	parent context.Context,
	ref processRef,
	entry *enginev1.ProcessInboxEntry,
	engine ProcessEngine,
	instances tables.ProcessInstanceTable,
	proposer Proposer,
	log *slog.Logger,
) *processSession {
	ctx, cancel := context.WithCancel(parent)
	return &processSession{
		ref:       ref,
		entry:     entry,
		engine:    engine,
		instances: instances,
		proposer:  proposer,
		log:       log,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
}

func (s *processSession) start()                { go s.run() }
func (s *processSession) abort()                { s.cancel() }
func (s *processSession) Done() <-chan struct{} { return s.done }

func (s *processSession) run() {
	defer close(s.done)
	if s.ctx.Err() != nil {
		return
	}

	lp := keys.LPFromPartitionKey(s.ref.pk)
	rec, ok, err := s.instances.Get(lp, s.ref.service, s.ref.instanceKey)
	if err != nil {
		s.log.Warn("invoker.proc: load record failed",
			"service", s.ref.service, "key", s.ref.instanceKey, "err", err)
		return
	}
	if !ok {
		// Instance reaped/transferred out from under the turn. The inbox row
		// is cleared by the same lifecycle that removed the record, so no turn
		// is stranded by returning here.
		s.log.Warn("invoker.proc: instance absent; dropping turn",
			"service", s.ref.service, "key", s.ref.instanceKey)
		return
	}

	adv, err := s.engine.Advance(s.ctx, ProcessAdvanceInput{
		Pk:          s.ref.pk,
		Service:     s.ref.service,
		InstanceKey: s.ref.instanceKey,
		Record:      rec,
		Entry:       s.entry,
	})
	if err != nil {
		// A model/evaluation failure is terminal for this one instance. Carry
		// the current blob unchanged plus a failed terminal so the apply path
		// marks the instance Failed and schedules reap, rather than stranding
		// the turn (which would wedge the inbox).
		s.log.Warn("invoker.proc: advance failed; failing instance",
			"service", s.ref.service, "key", s.ref.instanceKey, "err", err)
		adv = &enginev1.ProcessAdvanced{
			NewState: rec.GetStateBlob(),
			Terminal: &enginev1.ProcessTerminal{Failed: true, FailureMessage: err.Error()},
		}
	}
	// Stamp the addressing so the apply arm can find the instance row even if a
	// (future) engine implementation leaves it unset.
	adv.Pk, adv.Service, adv.InstanceKey = s.ref.pk, s.ref.service, s.ref.instanceKey

	if s.ctx.Err() != nil {
		return
	}
	cmd := &enginev1.Command{Kind: &enginev1.Command_ProcessAdvanced{ProcessAdvanced: adv}}
	propCtx, cancel := context.WithTimeout(s.ctx, proposeTimeout)
	defer cancel()
	if err := s.proposer.ProposeSelf(propCtx, cmd); err != nil && s.ctx.Err() == nil {
		// No ProcessAdvanced committed → the inbox still holds this event as
		// the active turn, and the next leader's resume re-drives it. Safe
		// because Advance is pure w.r.t. (record, event, logical_time) and the
		// ProcessAdvanced apply arm mints deterministic ids for any dispatched
		// task/child, so a re-applied turn dedups instead of double-firing.
		s.log.Warn("invoker.proc: propose ProcessAdvanced failed",
			"service", s.ref.service, "key", s.ref.instanceKey, "err", err)
	}
}
