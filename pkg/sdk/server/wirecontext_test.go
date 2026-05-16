package server

import (
	"errors"
	"io"
	"testing"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// fakeStream captures frames sent by the wireContext under test. Recv
// returns io.EOF — wireContext never reads from the stream, so this is
// a safety stub.
type fakeStream struct {
	sent []*protocolv1.Frame
}

func (f *fakeStream) Send(frame *protocolv1.Frame) error {
	f.sent = append(f.sent, frame)
	return nil
}

func (f *fakeStream) Recv() (*protocolv1.Frame, error) { return nil, io.EOF }

// newTestWireContext returns a wireContext backed by a fakeStream plus
// the default protobuf codec. Tests inspect fakeStream.sent to assert
// the emitted frame shapes. cache may be nil for tests that don't
// exercise GetState.
func newTestWireContext(t *testing.T, cache map[string][]byte) (*wireContext, *fakeStream) {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, handlerclient.DefaultCodec(), cache)
	return wctx, stream
}

// TestWireContext_InputAndID confirms the three load-bearing accessors
// return what the constructor was handed.
func TestWireContext_InputAndID(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)
	if got := wctx.Input(); string(got) != "hello" {
		t.Errorf("Input() = %q; want %q", got, "hello")
	}
	if got := wctx.InvocationID(); string(got.GetUuid()) != "0123456789ABCDEF" {
		t.Errorf("InvocationID().Uuid = %q; want %q", got.GetUuid(), "0123456789ABCDEF")
	}
	if wctx.Context() == nil {
		t.Errorf("Context() returned nil")
	}
}

// TestWireContext_StateWrites covers SetState / ClearState / ClearAllState.
// Each method emits the matching protocolv1 command frame and returns nil.
func TestWireContext_StateWrites(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	if err := wctx.SetState("counter", []byte("42")); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	if err := wctx.ClearState("temp"); err != nil {
		t.Fatalf("ClearState: %v", err)
	}
	if err := wctx.ClearAllState(); err != nil {
		t.Fatalf("ClearAllState: %v", err)
	}

	if got, want := len(stream.sent), 3; got != want {
		t.Fatalf("sent frames = %d; want %d", got, want)
	}

	// SetState frame.
	tc, _, _ := handlerclient.UnpackHeader(stream.sent[0].GetHeader())
	if tc != handlerclient.TypeCmdSetState {
		t.Errorf("frame[0].type = 0x%04x; want 0x%04x", tc, handlerclient.TypeCmdSetState)
	}
	var setMsg protocolv1.SetStateCommandMessage
	if err := handlerclient.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &setMsg); err != nil {
		t.Fatalf("decode SetStateCommandMessage: %v", err)
	}
	if got := string(setMsg.GetKey()); got != "counter" {
		t.Errorf("set.key = %q; want %q", got, "counter")
	}
	if got := string(setMsg.GetValue().GetContent()); got != "42" {
		t.Errorf("set.value = %q; want %q", got, "42")
	}

	// ClearState frame.
	tc, _, _ = handlerclient.UnpackHeader(stream.sent[1].GetHeader())
	if tc != handlerclient.TypeCmdClearState {
		t.Errorf("frame[1].type = 0x%04x; want 0x%04x", tc, handlerclient.TypeCmdClearState)
	}
	var clrMsg protocolv1.ClearStateCommandMessage
	if err := handlerclient.DefaultCodec().Unmarshal(stream.sent[1].GetPayload(), &clrMsg); err != nil {
		t.Fatalf("decode ClearStateCommandMessage: %v", err)
	}
	if got := string(clrMsg.GetKey()); got != "temp" {
		t.Errorf("clear.key = %q; want %q", got, "temp")
	}

	// ClearAllState frame.
	tc, _, _ = handlerclient.UnpackHeader(stream.sent[2].GetHeader())
	if tc != handlerclient.TypeCmdClearAllState {
		t.Errorf("frame[2].type = 0x%04x; want 0x%04x", tc, handlerclient.TypeCmdClearAllState)
	}
}

// TestWireContext_GetState_FromPreload serves GetState directly from the
// eager state_map snapshot delivered via StartMessage. Hits return the
// value + present=true; misses return (nil, false, nil) (NOT
// ErrWireNotImplemented — the cache is the authoritative source after
// 5f.2).
func TestWireContext_GetState_FromPreload(t *testing.T) {
	wctx, _ := newTestWireContext(t, map[string][]byte{
		"counter":  []byte("42"),
		"language": []byte("go"),
	})

	v, ok, err := wctx.GetState("counter")
	if err != nil {
		t.Fatalf("GetState(counter): %v", err)
	}
	if !ok {
		t.Fatal("GetState(counter): present=false; want true")
	}
	if got := string(v); got != "42" {
		t.Errorf("GetState(counter) = %q; want %q", got, "42")
	}

	_, ok, err = wctx.GetState("missing")
	if err != nil {
		t.Fatalf("GetState(missing): %v", err)
	}
	if ok {
		t.Error("GetState(missing): present=true; want false")
	}
}

// TestWireContext_GetState_CacheCoherentWithWrites confirms that
// SetState / ClearState / ClearAllState keep the eager cache coherent
// so a subsequent GetState in the same session observes the write
// without a round-trip.
func TestWireContext_GetState_CacheCoherentWithWrites(t *testing.T) {
	wctx, _ := newTestWireContext(t, map[string][]byte{"existing": []byte("old")})

	// SetState adds a new key.
	if err := wctx.SetState("fresh", []byte("hello")); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	v, ok, _ := wctx.GetState("fresh")
	if !ok || string(v) != "hello" {
		t.Errorf("after SetState(fresh): GetState=(%q,%v); want (hello, true)", v, ok)
	}

	// SetState overwrites an existing key.
	if err := wctx.SetState("existing", []byte("new")); err != nil {
		t.Fatalf("SetState(existing): %v", err)
	}
	v, ok, _ = wctx.GetState("existing")
	if !ok || string(v) != "new" {
		t.Errorf("after SetState(existing): GetState=(%q,%v); want (new, true)", v, ok)
	}

	// ClearState removes a key.
	if err := wctx.ClearState("fresh"); err != nil {
		t.Fatalf("ClearState: %v", err)
	}
	if _, ok, _ := wctx.GetState("fresh"); ok {
		t.Error("after ClearState(fresh): present=true; want false")
	}

	// ClearAllState wipes everything.
	if err := wctx.ClearAllState(); err != nil {
		t.Fatalf("ClearAllState: %v", err)
	}
	if _, ok, _ := wctx.GetState("existing"); ok {
		t.Error("after ClearAllState: existing still present; want gone")
	}
}

// TestWireContext_GetState_NilCache returns (nil, false, nil) when no
// state_map was preloaded (unkeyed services, or the eager preload
// overflowed and got dropped engine-side).
func TestWireContext_GetState_NilCache(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)
	v, ok, err := wctx.GetState("anything")
	if err != nil {
		t.Errorf("GetState err = %v; want nil", err)
	}
	if ok || v != nil {
		t.Errorf("GetState = (%q, %v); want (nil, false)", v, ok)
	}
}

// TestWireContext_SlotAllocation verifies allocSlot advances by span and
// matches inproc's contract: slot 0 is reserved for JEInput; user calls
// start at slot 1.
func TestWireContext_SlotAllocation(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)

	if got := wctx.allocSlot(1); got != 1 {
		t.Errorf("first allocSlot(1) = %d; want 1", got)
	}
	if got := wctx.allocSlot(2); got != 2 {
		t.Errorf("second allocSlot(2) = %d; want 2", got)
	}
	if got := wctx.allocSlot(1); got != 4 {
		t.Errorf("third allocSlot(1) = %d; want 4", got)
	}
}

// TestWireContext_DurablePrimitivesNotImplemented covers every durable
// primitive still gated on the 5f.2-5f.6 wire-protocol expansion.
func TestWireContext_DurablePrimitivesNotImplemented(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)

	for _, tc := range []struct {
		name   string
		future sdk.Future
	}{
		{"Sleep", wctx.Sleep(0)},
		{"Call", wctx.Call(sdk.Target{}, nil)},
	} {
		_, err := tc.future.Result()
		if !errors.Is(err, ErrWireNotImplemented) {
			t.Errorf("%s.Result() err = %v; want ErrWireNotImplemented", tc.name, err)
		}
	}

	_, akFuture := wctx.Awakeable()
	if _, err := akFuture.Result(); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("Awakeable.Result() err = %v; want ErrWireNotImplemented", err)
	}

	if _, err := wctx.Run("x", func() ([]byte, error) { return nil, nil }); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("Run err = %v; want ErrWireNotImplemented", err)
	}
	if err := wctx.OneWayCall(sdk.Target{}, nil); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("OneWayCall err = %v; want ErrWireNotImplemented", err)
	}
	if err := wctx.SendSignal(sdk.Target{}, "s", nil); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("SendSignal err = %v; want ErrWireNotImplemented", err)
	}

	all := wctx.All(notImplementedFuture{})
	if _, err := all.Results(); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("All.Results err = %v; want ErrWireNotImplemented", err)
	}
	any := wctx.Any(notImplementedFuture{})
	if _, err := any.Result(); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("Any.Result err = %v; want ErrWireNotImplemented", err)
	}
}

var _ frameStream = (*fakeStream)(nil)
