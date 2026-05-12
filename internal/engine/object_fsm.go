package engine

import (
	"context"
	"fmt"
	"reflect"

	"github.com/qmuntal/stateless"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Per-key VO FSM. Constructed fresh inside each partition apply call,
// configured against a working copy of the KeyLeaseStatus, and Fire'd to
// advance the gate for one event. The caller persists the resulting status
// via tables.KeyLeaseTable.Put in the same Pebble batch as the invocation
// status transition that triggered the fire.
//
// States and triggers are package-private — only the partition apply path
// drives this FSM.

type vobjState int

const (
	vobjIdle vobjState = iota
	vobjActive
)

type vobjTrigger int

const (
	vobjEnqueue vobjTrigger = iota // args: *enginev1.InvocationId
	vobjComplete
)

// buildObjectFSM returns a state machine bound to a working copy of cur and
// the working copy itself. The caller fires triggers and then writes the
// working copy to KeyLeaseTable.
//
// onActivate is invoked whenever an invocation is granted the lease — the
// partition apply path uses this to push ActInvoke on the collector.
func buildObjectFSM(
	cur *enginev1.KeyLeaseStatus,
	onActivate func(*enginev1.InvocationId),
) (*stateless.StateMachine, *enginev1.KeyLeaseStatus) {
	var next *enginev1.KeyLeaseStatus
	if cur == nil {
		next = &enginev1.KeyLeaseStatus{State: enginev1.KeyLeaseStatus_IDLE}
	} else {
		next = proto.Clone(cur).(*enginev1.KeyLeaseStatus)
	}

	sm := stateless.NewStateMachineWithExternalStorage(
		func(_ context.Context) (stateless.State, error) {
			return protoStateToVobj(next.GetState()), nil
		},
		func(_ context.Context, s stateless.State) error {
			next.State = vobjStateToProto(s.(vobjState))
			return nil
		},
		stateless.FiringImmediate,
	)
	sm.SetTriggerParameters(vobjEnqueue, reflect.TypeFor[*enginev1.InvocationId]())

	sm.Configure(vobjIdle).
		Permit(vobjEnqueue, vobjActive)

	sm.Configure(vobjActive).
		// Enqueue while active: append to the FIFO; no state change, no
		// onActivate (the current invocation is still running).
		InternalTransition(vobjEnqueue, func(_ context.Context, args ...any) error {
			id := args[0].(*enginev1.InvocationId)
			next.Queue = append(next.Queue, id)
			return nil
		}).
		// Complete while active: if the queue is empty fall through to Idle;
		// otherwise re-enter Active so the OnEntryFrom(Complete) hook pops
		// the head and activates it.
		PermitDynamic(vobjComplete, func(_ context.Context, _ ...any) (stateless.State, error) {
			if len(next.GetQueue()) == 0 {
				return vobjIdle, nil
			}
			return vobjActive, nil
		}).
		// OnEntry from Enqueue fires only on Idle → Active (the initial
		// activation): record the invocation as the lease holder and
		// activate it.
		OnEntryFrom(vobjEnqueue, func(_ context.Context, args ...any) error {
			id := args[0].(*enginev1.InvocationId)
			next.CurrentInvocation = id
			onActivate(id)
			return nil
		}).
		// OnEntry from Complete fires only on Active → Active reentry: pop
		// the head of the queue and activate it.
		OnEntryFrom(vobjComplete, func(_ context.Context, _ ...any) error {
			if len(next.Queue) == 0 {
				return fmt.Errorf("vobj: complete reentered Active with empty queue")
			}
			head := next.Queue[0]
			next.Queue = next.Queue[1:]
			next.CurrentInvocation = head
			onActivate(head)
			return nil
		}).
		// Active → Idle on Complete: clear the current invocation.
		OnExitWith(vobjComplete, func(_ context.Context, _ ...any) error {
			if len(next.GetQueue()) == 0 {
				next.CurrentInvocation = nil
			}
			return nil
		})

	return sm, next
}

func protoStateToVobj(s enginev1.KeyLeaseStatus_State) vobjState {
	if s == enginev1.KeyLeaseStatus_ACTIVE {
		return vobjActive
	}
	return vobjIdle
}

func vobjStateToProto(s vobjState) enginev1.KeyLeaseStatus_State {
	if s == vobjActive {
		return enginev1.KeyLeaseStatus_ACTIVE
	}
	return enginev1.KeyLeaseStatus_IDLE
}
