package invoker

import (
	"sync"
	"testing"

	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// abortProbeSession is a sessionHandle stub that records abort() and never
// closes Done — so TriggerAbort hanging on <-Done() would deadlock the test.
type abortProbeSession struct {
	mu      sync.Mutex
	aborted bool
	done    chan struct{}
}

func (s *abortProbeSession) start() {}
func (s *abortProbeSession) abort() {
	s.mu.Lock()
	s.aborted = true
	s.mu.Unlock()
}
func (s *abortProbeSession) Done() <-chan struct{} { return s.done }
func (s *abortProbeSession) wasAborted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.aborted
}

// TestInvoker_TriggerAbort_SignalsNonBlocking: TriggerAbort signals the live
// session's abort() and returns without waiting on Done() (the probe never
// closes Done, so a blocking impl would hang the test).
func TestInvoker_TriggerAbort_SignalsNonBlocking(t *testing.T) {
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("trigger-abort--16")[:16]}
	s := &abortProbeSession{done: make(chan struct{})}
	i := &Invoker{sessions: map[string]sessionHandle{sessionKey(id): s}}

	i.TriggerAbort(id) // must not block

	if !s.wasAborted() {
		t.Fatal("TriggerAbort did not signal abort() on the live session")
	}
}

// TestInvoker_TriggerAbort_NoSessionNoop: TriggerAbort for an id with no live
// session is a clean no-op (the common case — Scheduled/Suspended/terminal
// invocations carry no session).
func TestInvoker_TriggerAbort_NoSessionNoop(t *testing.T) {
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("trigger-noop---16")[:16]}
	i := &Invoker{sessions: map[string]sessionHandle{}}
	i.TriggerAbort(id) // must not panic
}
