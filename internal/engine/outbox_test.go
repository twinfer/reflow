package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/tables"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// fakeIngressProposer captures (producerID, seq, cmd) tuples for assertions.
type fakeIngressProposer struct {
	mu       sync.Mutex
	calls    []fakeIngressCall
	failNext int   // fail the next N calls with a transient-style error
	failWith error // what to return when failNext > 0
}

type fakeIngressCall struct {
	ProducerID string
	Seq        uint64
	Cmd        *enginev1.Command
}

func (f *fakeIngressProposer) ProposeIngress(_ context.Context, producerID string, seq uint64, cmd *enginev1.Command) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return f.failWith
	}
	f.calls = append(f.calls, fakeIngressCall{ProducerID: producerID, Seq: seq, Cmd: cmd})
	return nil
}

func (f *fakeIngressProposer) snapshot() []fakeIngressCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeIngressCall, len(f.calls))
	copy(out, f.calls)
	return out
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestOutbox_FIFOOrdering(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	ot := tables.OutboxTable{S: s}

	// Seed the table out-of-order: 3, 1, 2.
	seqs := []uint64{3, 1, 2}
	for _, seq := range seqs {
		b := s.NewBatch()
		env := &enginev1.OutboxEnvelope{
			Kind: &enginev1.OutboxEnvelope_Invoke{Invoke: &enginev1.InvokeCommand{
				InvocationId: &enginev1.InvocationId{PartitionKey: seq, Uuid: []byte("0123456789abcdef")},
				Target:       &enginev1.InvocationTarget{ServiceName: "S"},
			}},
		}
		if err := ot.Append(b, seq, env); err != nil {
			t.Fatal(err)
		}
		if err := b.Commit(true); err != nil {
			t.Fatal(err)
		}
		b.Close()
	}

	fp := &fakeIngressProposer{}
	svc := NewOutboxService(ot, fp, nil, 1, discardLogger(), nil)

	// Rebuild loads the table into pending.
	if err := svc.Rebuild(); err != nil {
		t.Fatal(err)
	}

	// Run until pending drains.
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-svc.Done()
	})

	waitFor(t, func() bool {
		return len(fp.snapshot()) == 3
	}, "shuffler to propose all 3 rows")

	calls := fp.snapshot()
	if calls[0].Seq != 1 || calls[1].Seq != 2 || calls[2].Seq != 3 {
		t.Errorf("propose order: %v; want [1,2,3]", []uint64{calls[0].Seq, calls[1].Seq, calls[2].Seq})
	}
	for _, c := range calls {
		if c.ProducerID != "outbox/p1" {
			t.Errorf("producerID = %q; want outbox/p1", c.ProducerID)
		}
		if c.Cmd.GetInvoke() == nil {
			t.Errorf("seq=%d: expected Invoke cmd; got %T", c.Seq, c.Cmd.GetKind())
		}
	}
}

func TestOutbox_PushDrainsLiveRows(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	ot := tables.OutboxTable{S: s}

	fp := &fakeIngressProposer{}
	svc := NewOutboxService(ot, fp, nil, 7, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-svc.Done()
	})

	// Live push (no Rebuild needed for new entries).
	env := &enginev1.OutboxEnvelope{
		Kind: &enginev1.OutboxEnvelope_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")},
			Target:       &enginev1.InvocationTarget{ServiceName: "Live"},
		}},
	}
	svc.Push(42, env)

	waitFor(t, func() bool {
		return len(fp.snapshot()) == 1
	}, "shuffler to propose the live row")

	calls := fp.snapshot()
	if calls[0].Seq != 42 || calls[0].ProducerID != "outbox/p7" {
		t.Errorf("call = %+v; want seq=42 producer=outbox/p7", calls[0])
	}
}

func TestOutbox_SignalEnvelopeConvertsToInvokerEffect(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	ot := tables.OutboxTable{S: s}
	fp := &fakeIngressProposer{}
	svc := NewOutboxService(ot, fp, nil, 1, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-svc.Done()
	})

	target := &enginev1.InvocationTarget{
		ServiceName: "Counter",
		HandlerName: "Increment",
		ObjectKey:   "alice",
	}
	env := &enginev1.OutboxEnvelope{
		Kind: &enginev1.OutboxEnvelope_Signal{Signal: &enginev1.SignalSend{
			Target:     target,
			SignalName: "ping",
			Payload:    []byte("hi"),
		}},
	}
	svc.Push(11, env)

	waitFor(t, func() bool {
		return len(fp.snapshot()) == 1
	}, "shuffler to propose the signal row")

	call := fp.snapshot()[0]
	eff := call.Cmd.GetInvokerEffect()
	if eff == nil {
		t.Fatalf("expected InvokerEffect cmd; got %T", call.Cmd.GetKind())
	}
	// InvokerEffect.invocation_id is intentionally nil for signal_delivered:
	// the sender knows only the Target. The receiver shard resolves it via
	// KeyLeaseTable in its apply arm.
	if eff.GetInvocationId() != nil {
		t.Errorf("InvokerEffect.invocation_id = %+v; want nil for SignalDelivered", eff.GetInvocationId())
	}
	sig := eff.GetSignalDelivered()
	if sig == nil {
		t.Fatalf("expected SignalDelivered effect; got %T", eff.GetKind())
	}
	if got := sig.GetTarget(); got.GetServiceName() != "Counter" || got.GetObjectKey() != "alice" {
		t.Errorf("signal target = %+v; want Counter/alice", got)
	}
	if sig.GetSignalName() != "ping" || string(sig.GetPayload()) != "hi" {
		t.Errorf("signal payload mismatch: %+v", sig)
	}
}

func TestOutbox_IdempotentReinjectionAfterCrash(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	ot := tables.OutboxTable{S: s}

	// Seed: two rows already in the table at restart time. The receiver
	// would normally pop them on apply, but we're simulating "shuffler
	// crashed between propose and receiver-side commit" by leaving them
	// in place. Rebuild must re-load them and the shuffler must re-propose.
	for _, seq := range []uint64{1, 2} {
		b := s.NewBatch()
		env := &enginev1.OutboxEnvelope{
			Kind: &enginev1.OutboxEnvelope_Invoke{Invoke: &enginev1.InvokeCommand{
				InvocationId: &enginev1.InvocationId{PartitionKey: seq, Uuid: []byte("0123456789abcdef")},
				Target:       &enginev1.InvocationTarget{ServiceName: "S"},
			}},
		}
		if err := ot.Append(b, seq, env); err != nil {
			t.Fatal(err)
		}
		_ = b.Commit(true)
		b.Close()
	}

	fp := &fakeIngressProposer{}
	svc := NewOutboxService(ot, fp, nil, 1, discardLogger(), nil)
	if err := svc.Rebuild(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-svc.Done()
	})

	waitFor(t, func() bool {
		return len(fp.snapshot()) == 2
	}, "re-injection of both rows after rebuild")

	calls := fp.snapshot()
	if calls[0].Seq != 1 || calls[1].Seq != 2 {
		t.Errorf("expected seqs [1,2]; got [%d,%d]", calls[0].Seq, calls[1].Seq)
	}
	// Dedup absorbs duplicates downstream — the test verifies the SHUFFLER
	// does emit them. The receiver-side dedup test lives in the integration
	// suite.
}

func TestOutbox_PropagateFailureAndRetry(t *testing.T) {
	s := storage.NewMemStore()
	defer s.Close()
	ot := tables.OutboxTable{S: s}

	// Fail the first 2 propose calls; succeed on the third.
	fp := &fakeIngressProposer{failNext: 2, failWith: errors.New("transient")}
	svc := NewOutboxService(ot, fp, nil, 1, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-svc.Done()
	})

	env := &enginev1.OutboxEnvelope{
		Kind: &enginev1.OutboxEnvelope_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789abcdef")},
			Target:       &enginev1.InvocationTarget{ServiceName: "S"},
		}},
	}
	svc.Push(99, env)

	waitFor(t, func() bool {
		return len(fp.snapshot()) == 1
	}, "successful propose after retries")

	calls := fp.snapshot()
	if len(calls) != 1 || calls[0].Seq != 99 {
		t.Errorf("calls = %+v", calls)
	}
	// pendingLen drops to 0 after the success.
	if l := svc.pendingLen(); l != 0 {
		t.Errorf("pending = %d; want 0", l)
	}
}

func TestIsOutboxProducer(t *testing.T) {
	cases := map[string]bool{
		"outbox/p1":    true,
		"outbox/p123":  true,
		"http/abc":     false,
		"":             false,
		"outboxxxx/p1": false,
		"outbox":       false,
	}
	for in, want := range cases {
		if got := isOutboxProducer(in); got != want {
			t.Errorf("isOutboxProducer(%q) = %v; want %v", in, got, want)
		}
	}
}
