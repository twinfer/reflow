package server

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// decodeBase64URL decodes the base64url body of a minted awakeable id.
func decodeBase64URL(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}

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
// exercise GetState; replay may be nil for tests that don't exercise
// the replay-skip path.
func newTestWireContext(t *testing.T, cache map[string][]byte) (*wireContext, *fakeStream) {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, handlerclient.DefaultCodec(), cache, nil, 7)
	return wctx, stream
}

// newTestWireContextWithReplay seeds the replay buffer so tests can
// assert replay-hit branches of Sleep, SetState, etc.
func newTestWireContextWithReplay(t *testing.T, replay map[uint32]*replayEntry) (*wireContext, *fakeStream) {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, handlerclient.DefaultCodec(), nil, replay, 7)
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
// state_map was preloaded (unkeyed services).
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

// TestWireContext_GetState_PartialMiss returns
// ErrLazyStateUnavailable on a miss when StartMessage.partial_state was
// true. Lazy fetch isn't wired, so the handler must distinguish "key
// absent" from "snapshot incomplete."
func TestWireContext_GetState_PartialMiss(t *testing.T) {
	wctx, _ := newTestWireContext(t, map[string][]byte{"present": []byte("v")})
	wctx.partialState = true

	if v, ok, err := wctx.GetState("present"); err != nil || !ok || string(v) != "v" {
		t.Errorf("present-key GetState = (%q, %v, %v); want (v, true, nil)", v, ok, err)
	}
	v, ok, err := wctx.GetState("missing")
	if !errors.Is(err, ErrLazyStateUnavailable) {
		t.Errorf("partial-miss GetState err = %v; want ErrLazyStateUnavailable", err)
	}
	if ok || v != nil {
		t.Errorf("partial-miss GetState = (%q, %v); want (nil, false)", v, ok)
	}
}

// TestWireContext_GetState_PartialNoCache surfaces
// ErrLazyStateUnavailable when partialState is true and there is no
// cache map at all (engine declined to send any state_map entries).
func TestWireContext_GetState_PartialNoCache(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)
	wctx.partialState = true

	_, _, err := wctx.GetState("anything")
	if !errors.Is(err, ErrLazyStateUnavailable) {
		t.Errorf("GetState err = %v; want ErrLazyStateUnavailable", err)
	}
}

// TestWireContext_SlotAllocation verifies allocSlot advances by span and
// matches inproc's contract: slot 0 is reserved for JEInput; user calls
// start at slot 1.
func TestWireContext_SlotAllocation(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)

	got, ok := wctx.allocSlot(1)
	if !ok || got != 1 {
		t.Errorf("first allocSlot(1) = (%d, %v); want (1, true)", got, ok)
	}
	got, ok = wctx.allocSlot(2)
	if !ok || got != 2 {
		t.Errorf("second allocSlot(2) = (%d, %v); want (2, true)", got, ok)
	}
	got, ok = wctx.allocSlot(1)
	if !ok || got != 4 {
		t.Errorf("third allocSlot(1) = (%d, %v); want (4, true)", got, ok)
	}
}

// TestWireContext_Sleep_FreshEmits asserts the first call to Sleep on
// a session with empty replay emits SleepCommandMessage and suspends.
func TestWireContext_Sleep_FreshEmits(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	fut := wctx.Sleep(50 * time.Millisecond)
	_, err := fut.Result()
	if !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("Sleep.Result err = %v; want ErrSuspended", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1 (SleepCommandMessage)", len(stream.sent))
	}
	tc, _, _ := handlerclient.UnpackHeader(stream.sent[0].GetHeader())
	if tc != handlerclient.TypeCmdSleep {
		t.Errorf("frame.type = 0x%04x; want 0x%04x", tc, handlerclient.TypeCmdSleep)
	}
	var msg protocolv1.SleepCommandMessage
	if err := handlerclient.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &msg); err != nil {
		t.Fatalf("decode SleepCommandMessage: %v", err)
	}
	if msg.GetResultCompletionId() != 2 {
		t.Errorf("result_completion_id = %d; want 2", msg.GetResultCompletionId())
	}
	if msg.GetWakeUpTime() == 0 {
		t.Errorf("wake_up_time = 0; want non-zero absolute ms")
	}

	awaiting := wctx.snapshotAwaiting()
	if len(awaiting) != 1 || awaiting[0] != "completion:2" {
		t.Errorf("awaiting = %v; want [completion:2]", awaiting)
	}
}

// TestWireContext_Sleep_ReplayHitReturnsReady asserts that when the
// replay buffer contains the SleepCompletionNotificationMessage for
// this slot, Sleep returns a ready future without emitting any frame.
func TestWireContext_Sleep_ReplayHitReturnsReady(t *testing.T) {
	codec := handlerclient.DefaultCodec()
	cmdPayload, err := codec.Marshal(&protocolv1.SleepCommandMessage{
		WakeUpTime:         12345,
		ResultCompletionId: 2,
	})
	if err != nil {
		t.Fatalf("marshal SleepCommandMessage: %v", err)
	}
	notePayload, err := codec.Marshal(&protocolv1.SleepCompletionNotificationMessage{
		CompletionId: 2,
		Void:         &protocolv1.Void{},
	})
	if err != nil {
		t.Fatalf("marshal SleepCompletionNotificationMessage: %v", err)
	}
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: handlerclient.TypeCmdSleep, payload: cmdPayload},
		2: {typeCode: handlerclient.TypeNoteSleepDone, payload: notePayload},
	})

	fut := wctx.Sleep(50 * time.Millisecond)
	v, err := fut.Result()
	if err != nil {
		t.Errorf("Sleep.Result err = %v; want nil", err)
	}
	if v != nil {
		t.Errorf("Sleep.Result value = %q; want nil", v)
	}
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (replay hit should not re-emit)", len(stream.sent))
	}
}

// TestWireContext_StateWrites_ReplayHitSkipsEmit confirms a SetState
// at a slot already covered by replay is a no-op on the wire (the
// engine already journaled it in a prior run).
func TestWireContext_StateWrites_ReplayHitSkipsEmit(t *testing.T) {
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: handlerclient.TypeCmdSetState, payload: nil},
	})

	if err := wctx.SetState("counter", []byte("42")); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (replay hit should not re-emit)", len(stream.sent))
	}
	// Cache update still happens so read-your-writes in the same session works.
	v, ok, _ := wctx.GetState("counter")
	if !ok || string(v) != "42" {
		t.Errorf("GetState(counter) after replay-skipped SetState = (%q, %v); want (42, true)", v, ok)
	}
}

// TestWireContext_Call_FreshEmits asserts the first call to Call on a
// session with empty replay emits CallCommandMessage and suspends with
// the matching completion token.
func TestWireContext_Call_FreshEmits(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	fut := wctx.Call(sdk.Target{Service: "Echo", Handler: "echo", Key: "k1"}, []byte("hi"))
	_, err := fut.Result()
	if !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("Call.Result err = %v; want ErrSuspended", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1 (CallCommandMessage)", len(stream.sent))
	}
	tc, _, _ := handlerclient.UnpackHeader(stream.sent[0].GetHeader())
	if tc != handlerclient.TypeCmdCall {
		t.Errorf("frame.type = 0x%04x; want 0x%04x", tc, handlerclient.TypeCmdCall)
	}
	var msg protocolv1.CallCommandMessage
	if err := handlerclient.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &msg); err != nil {
		t.Fatalf("decode CallCommandMessage: %v", err)
	}
	if msg.GetServiceName() != "Echo" || msg.GetHandlerName() != "echo" || msg.GetKey() != "k1" {
		t.Errorf("decoded target = %s/%s[%s]; want Echo/echo[k1]",
			msg.GetServiceName(), msg.GetHandlerName(), msg.GetKey())
	}
	if string(msg.GetParameter()) != "hi" {
		t.Errorf("parameter = %q; want %q", msg.GetParameter(), "hi")
	}
	if msg.GetResultCompletionId() != 2 {
		t.Errorf("result_completion_id = %d; want 2", msg.GetResultCompletionId())
	}
}

// TestWireContext_Call_ReplayHitReturnsValue asserts a CallCompletion
// notification in the replay buffer surfaces as a ready future
// carrying the value, with no fresh frame on the wire.
func TestWireContext_Call_ReplayHitReturnsValue(t *testing.T) {
	codec := handlerclient.DefaultCodec()
	notePayload, err := codec.Marshal(&protocolv1.CallCompletionNotificationMessage{
		CompletionId: 2,
		Result: &protocolv1.CallCompletionNotificationMessage_Value{
			Value: &protocolv1.Value{Content: []byte("answer")},
		},
	})
	if err != nil {
		t.Fatalf("marshal note: %v", err)
	}
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: handlerclient.TypeCmdCall},
		2: {typeCode: handlerclient.TypeNoteCallDone, payload: notePayload},
	})

	v, err := wctx.Call(sdk.Target{Service: "X", Handler: "y"}, nil).Result()
	if err != nil {
		t.Errorf("Call.Result err = %v; want nil", err)
	}
	if string(v) != "answer" {
		t.Errorf("Call.Result value = %q; want %q", v, "answer")
	}
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (replay hit should not re-emit)", len(stream.sent))
	}
}

// TestWireContext_Call_ReplayHitSurfacesFailure asserts a CallCompletion
// with a Failure result becomes a terminal failure via the returned
// future's error.
func TestWireContext_Call_ReplayHitSurfacesFailure(t *testing.T) {
	codec := handlerclient.DefaultCodec()
	notePayload, err := codec.Marshal(&protocolv1.CallCompletionNotificationMessage{
		CompletionId: 2,
		Result: &protocolv1.CallCompletionNotificationMessage_Failure{
			Failure: &protocolv1.Failure{Code: 42, Message: "callee failed"},
		},
	})
	if err != nil {
		t.Fatalf("marshal note: %v", err)
	}
	wctx, _ := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: handlerclient.TypeCmdCall},
		2: {typeCode: handlerclient.TypeNoteCallDone, payload: notePayload},
	})

	_, err = wctx.Call(sdk.Target{Service: "X", Handler: "y"}, nil).Result()
	if err == nil {
		t.Fatal("Call.Result err = nil; want non-nil failure")
	}
	f, ok := sdk.AsFailure(err)
	if !ok {
		t.Fatalf("Call.Result err = %v; want *sdk.Failure", err)
	}
	if f.Code != 42 || f.Message != "callee failed" {
		t.Errorf("failure = (code=%d, msg=%q); want (42, callee failed)", f.Code, f.Message)
	}
}

// TestWireContext_OneWayCall_FreshEmits asserts OneWayCall emits an
// OneWayCallCommandMessage and returns nil — no suspension because
// there's no result to await.
func TestWireContext_OneWayCall_FreshEmits(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)
	if err := wctx.OneWayCall(sdk.Target{Service: "X", Handler: "y", Key: "k1"}, []byte("p")); err != nil {
		t.Fatalf("OneWayCall: %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1", len(stream.sent))
	}
	tc, _, _ := handlerclient.UnpackHeader(stream.sent[0].GetHeader())
	if tc != handlerclient.TypeCmdOneWayCall {
		t.Errorf("frame.type = 0x%04x; want 0x%04x", tc, handlerclient.TypeCmdOneWayCall)
	}
}

// TestWireContext_OneWayCall_ReplayHitSkipsEmit asserts a replay-hit
// OneWayCall is a no-op (engine already journaled it).
func TestWireContext_OneWayCall_ReplayHitSkipsEmit(t *testing.T) {
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: handlerclient.TypeCmdOneWayCall},
	})
	if err := wctx.OneWayCall(sdk.Target{Service: "X", Handler: "y"}, nil); err != nil {
		t.Errorf("OneWayCall: %v", err)
	}
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (replay hit should not re-emit)", len(stream.sent))
	}
}

// TestWireContext_Run_FreshExecutesAndEmitsBothFrames asserts the
// happy path: fn() runs locally, RunCommandMessage +
// ProposeRunCompletionMessage are emitted in order, and the inline
// return path surfaces fn's value.
func TestWireContext_Run_FreshExecutesAndEmitsBothFrames(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	ranCount := 0
	v, err := wctx.Run("compute", func() ([]byte, error) {
		ranCount++
		return []byte("answer"), nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if string(v) != "answer" {
		t.Errorf("Run value = %q; want %q", v, "answer")
	}
	if ranCount != 1 {
		t.Errorf("fn ran %d times; want 1", ranCount)
	}
	if len(stream.sent) != 2 {
		t.Fatalf("sent frames = %d; want 2 (Run + ProposeRun)", len(stream.sent))
	}
	tc, _, _ := handlerclient.UnpackHeader(stream.sent[0].GetHeader())
	if tc != handlerclient.TypeCmdRun {
		t.Errorf("frame[0].type = 0x%04x; want 0x%04x (TypeCmdRun)", tc, handlerclient.TypeCmdRun)
	}
	tc, _, _ = handlerclient.UnpackHeader(stream.sent[1].GetHeader())
	if tc != handlerclient.TypeProposeRunDone {
		t.Errorf("frame[1].type = 0x%04x; want 0x%04x (TypeProposeRunDone)", tc, handlerclient.TypeProposeRunDone)
	}
	var prop protocolv1.ProposeRunCompletionMessage
	if err := handlerclient.DefaultCodec().Unmarshal(stream.sent[1].GetPayload(), &prop); err != nil {
		t.Fatalf("decode ProposeRunCompletionMessage: %v", err)
	}
	if prop.GetRetryable() {
		t.Error("prop.retryable = true; want false on success")
	}
	val, ok := prop.GetResult().(*protocolv1.ProposeRunCompletionMessage_Value)
	if !ok {
		t.Fatalf("prop.result = %T; want Value", prop.GetResult())
	}
	if string(val.Value) != "answer" {
		t.Errorf("prop.value = %q; want %q", val.Value, "answer")
	}
}

// TestWireContext_Run_TransientErrorMarksRetryable asserts a non-Failure
// fn error sets retryable=true and the SDK suspends pending the
// engine's backoff timer.
func TestWireContext_Run_TransientErrorMarksRetryable(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	_, err := wctx.Run("fetch", func() ([]byte, error) {
		return nil, errors.New("network blip")
	})
	if !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("Run err = %v; want ErrSuspended (retryable)", err)
	}
	var prop protocolv1.ProposeRunCompletionMessage
	if err := handlerclient.DefaultCodec().Unmarshal(stream.sent[1].GetPayload(), &prop); err != nil {
		t.Fatalf("decode ProposeRunCompletionMessage: %v", err)
	}
	if !prop.GetRetryable() {
		t.Error("prop.retryable = false; want true for transient error")
	}
}

// TestWireContext_Run_FailureIsTerminal asserts a returned *sdk.Failure
// is recorded as terminal (retryable=false) and surfaced from Run.
func TestWireContext_Run_FailureIsTerminal(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	_, err := wctx.Run("validate", func() ([]byte, error) {
		return nil, sdk.NewFailure(0, "bad input")
	})
	f, ok := sdk.AsFailure(err)
	if !ok {
		t.Fatalf("Run err = %v; want *sdk.Failure", err)
	}
	if f.Message != "bad input" {
		t.Errorf("failure.message = %q; want %q", f.Message, "bad input")
	}
	var prop protocolv1.ProposeRunCompletionMessage
	if err := handlerclient.DefaultCodec().Unmarshal(stream.sent[1].GetPayload(), &prop); err != nil {
		t.Fatalf("decode ProposeRunCompletionMessage: %v", err)
	}
	if prop.GetRetryable() {
		t.Error("prop.retryable = true; want false for terminal failure")
	}
}

// TestWireContext_Run_ReplayHitReturnsCachedValue asserts a replayed
// RunCompletionNotificationMessage surfaces directly without
// re-executing fn.
func TestWireContext_Run_ReplayHitReturnsCachedValue(t *testing.T) {
	codec := handlerclient.DefaultCodec()
	notePayload, err := codec.Marshal(&protocolv1.RunCompletionNotificationMessage{
		CompletionId: 1,
		Result: &protocolv1.RunCompletionNotificationMessage_Value{
			Value: &protocolv1.Value{Content: []byte("cached")},
		},
	})
	if err != nil {
		t.Fatalf("marshal note: %v", err)
	}
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: handlerclient.TypeNoteRunDone, payload: notePayload},
	})

	ranCount := 0
	v, err := wctx.Run("compute", func() ([]byte, error) {
		ranCount++
		return []byte("fresh"), nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if string(v) != "cached" {
		t.Errorf("Run value = %q; want %q", v, "cached")
	}
	if ranCount != 0 {
		t.Errorf("fn ran %d times on replay-hit; want 0", ranCount)
	}
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 on replay-hit", len(stream.sent))
	}
}

// TestWireContext_Awakeable_FreshMintsAndSuspends asserts the first
// Awakeable call mints a partition_key-encoded id, emits
// AwakeableCommandMessage, and returns a suspended future.
func TestWireContext_Awakeable_FreshMintsAndSuspends(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	id, fut := wctx.Awakeable()
	if id == "" {
		t.Fatal("Awakeable returned empty id")
	}
	if got := id[:4]; got != "awk_" {
		t.Errorf("id prefix = %q; want awk_", got)
	}
	if len(id) != 26 {
		t.Errorf("id len = %d; want 26 (awk_ + 22 base64url)", len(id))
	}
	if _, err := fut.Result(); !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("Awakeable future.Result err = %v; want ErrSuspended", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1 (AwakeableCommandMessage)", len(stream.sent))
	}
	tc, _, _ := handlerclient.UnpackHeader(stream.sent[0].GetHeader())
	if tc != handlerclient.TypeCmdAwakeable {
		t.Errorf("frame.type = 0x%04x; want 0x%04x", tc, handlerclient.TypeCmdAwakeable)
	}
	var msg protocolv1.AwakeableCommandMessage
	if err := handlerclient.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &msg); err != nil {
		t.Fatalf("decode AwakeableCommandMessage: %v", err)
	}
	if msg.GetAwakeableId() != id {
		t.Errorf("AwakeableCommandMessage.id = %q; want %q", msg.GetAwakeableId(), id)
	}
	if msg.GetResultCompletionId() != 2 {
		t.Errorf("result_completion_id = %d; want 2", msg.GetResultCompletionId())
	}
}

// TestWireContext_Awakeable_ReplayHitWithSignal asserts a replayed
// AwakeableCommandMessage + SignalNotificationMessage pair surfaces
// the cached id + resolved value without re-minting or re-emitting.
func TestWireContext_Awakeable_ReplayHitWithSignal(t *testing.T) {
	codec := handlerclient.DefaultCodec()
	cmdPayload, err := codec.Marshal(&protocolv1.AwakeableCommandMessage{
		ResultCompletionId: 2,
		AwakeableId:        "awk_replayid12345678901234567",
	})
	if err != nil {
		t.Fatalf("marshal AwakeableCommandMessage: %v", err)
	}
	signalPayload, err := codec.Marshal(&protocolv1.SignalNotificationMessage{
		SignalId: &protocolv1.SignalNotificationMessage_Name{
			Name: "awk_replayid12345678901234567",
		},
		Result: &protocolv1.SignalNotificationMessage_Value{
			Value: &protocolv1.Value{Content: []byte("resolved")},
		},
	})
	if err != nil {
		t.Fatalf("marshal SignalNotificationMessage: %v", err)
	}
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: handlerclient.TypeCmdAwakeable, payload: cmdPayload},
		2: {typeCode: handlerclient.TypeNoteSignal, payload: signalPayload},
	})

	id, fut := wctx.Awakeable()
	if id != "awk_replayid12345678901234567" {
		t.Errorf("Awakeable id = %q; want %q", id, "awk_replayid12345678901234567")
	}
	v, err := fut.Result()
	if err != nil {
		t.Errorf("future.Result err = %v; want nil", err)
	}
	if string(v) != "resolved" {
		t.Errorf("future.Result value = %q; want %q", v, "resolved")
	}
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 on full replay-hit", len(stream.sent))
	}
}

// TestWireContext_Awakeable_ReplayHitCmdOnlyStillSuspends asserts an
// Awakeable that has its command journaled but no resolution yet
// returns the cached id + suspendedFuture.
func TestWireContext_Awakeable_ReplayHitCmdOnlyStillSuspends(t *testing.T) {
	codec := handlerclient.DefaultCodec()
	cmdPayload, err := codec.Marshal(&protocolv1.AwakeableCommandMessage{
		ResultCompletionId: 2,
		AwakeableId:        "awk_pending1234567890123456789",
	})
	if err != nil {
		t.Fatalf("marshal AwakeableCommandMessage: %v", err)
	}
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: handlerclient.TypeCmdAwakeable, payload: cmdPayload},
	})

	id, fut := wctx.Awakeable()
	if id != "awk_pending1234567890123456789" {
		t.Errorf("Awakeable id = %q; want cached id", id)
	}
	if _, err := fut.Result(); !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("future.Result err = %v; want ErrSuspended (cmd-only replay)", err)
	}
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (cmd already journaled)", len(stream.sent))
	}
}

// TestWireContext_Awakeable_IDEmbedsPartitionKey asserts the minted id
// encodes partitionKey in its first 8 bytes — the contract
// ingress.ResolveAwakeable depends on for routing.
func TestWireContext_Awakeable_IDEmbedsPartitionKey(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)
	id, _ := wctx.Awakeable()
	const prefix = "awk_"
	if got := id[:len(prefix)]; got != prefix {
		t.Fatalf("id prefix = %q; want %q", got, prefix)
	}
	decoded, err := decodeBase64URL(id[len(prefix):])
	if err != nil {
		t.Fatalf("decode id body: %v", err)
	}
	if len(decoded) != 16 {
		t.Fatalf("decoded len = %d; want 16", len(decoded))
	}
	got := binary.BigEndian.Uint64(decoded[:8])
	if got != 7 {
		t.Errorf("decoded partition_key = %d; want 7 (the fixture's)", got)
	}
}

// TestWireContext_Suspend_ShortCircuitsSubsequentCalls asserts that
// once suspended, every ctx call returns ErrSuspended (mirrors
// inprocContext.suspend).
func TestWireContext_Suspend_ShortCircuitsSubsequentCalls(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)

	// First Sleep call suspends.
	if _, err := wctx.Sleep(time.Second).Result(); !errors.Is(err, sdk.ErrSuspended) {
		t.Fatalf("first Sleep.Result err = %v; want ErrSuspended", err)
	}
	// SetState after suspend short-circuits without emitting.
	if err := wctx.SetState("k", []byte("v")); !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("SetState after suspend err = %v; want ErrSuspended", err)
	}
	// Second Sleep also short-circuits.
	if _, err := wctx.Sleep(time.Second).Result(); !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("second Sleep.Result err = %v; want ErrSuspended", err)
	}
}

// TestWireContext_SendSignalStillGated covers the one durable primitive
// that remains not-implemented: SendSignal needs a Target → InvocationId
// resolver (matching inproc.go's state).
func TestWireContext_SendSignalStillGated(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)

	if err := wctx.SendSignal(sdk.Target{}, "s", nil); !errors.Is(err, ErrWireNotImplemented) {
		t.Errorf("SendSignal err = %v; want ErrWireNotImplemented", err)
	}
}

// TestWireContext_AllAllChildrenReady returns the children's resolved
// values in argument order when every child is ready at suspend time.
func TestWireContext_AllAllChildrenReady(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)
	r := wctx.All(readyFuture{value: []byte("a")}, readyFuture{value: []byte("b")})
	out, err := r.Results()
	if err != nil {
		t.Fatalf("All.Results: %v", err)
	}
	if len(out) != 2 || string(out[0]) != "a" || string(out[1]) != "b" {
		t.Errorf("All.Results = %q; want [a b]", out)
	}
}

// TestWireContext_AllPendingSuspends: one ready, one suspended → All
// returns ErrSuspended without consulting the resolved child.
func TestWireContext_AllPendingSuspends(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)
	pending := sleepFuture{ctx: wctx, resultSlot: 99} // no replay at slot 99
	r := wctx.All(readyFuture{value: []byte("ok")}, pending)
	_, err := r.Results()
	if !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("All.Results err = %v; want sdk.ErrSuspended", err)
	}
	awaiting := wctx.snapshotAwaiting()
	if len(awaiting) != 1 || awaiting[0] != "completion:99" {
		t.Errorf("awaiting = %v; want [completion:99]", awaiting)
	}
}

// TestWireContext_AnyReadyShortCircuits: Any resolves to the first
// ready child even when others are pending.
func TestWireContext_AnyReadyShortCircuits(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)
	pending := sleepFuture{ctx: wctx, resultSlot: 99}
	r := wctx.Any(pending, readyFuture{value: []byte("winner")})
	out, err := r.Result()
	if err != nil {
		t.Fatalf("Any.Result: %v", err)
	}
	if string(out) != "winner" {
		t.Errorf("Any.Result = %q; want %q", out, "winner")
	}
}

// TestWireContext_AnyAllPendingSuspends: every child pending → Any
// returns ErrSuspended with the union of waker tokens.
func TestWireContext_AnyAllPendingSuspends(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)
	p1 := sleepFuture{ctx: wctx, resultSlot: 10}
	p2 := sleepFuture{ctx: wctx, resultSlot: 20}
	r := wctx.Any(p1, p2)
	_, err := r.Result()
	if !errors.Is(err, sdk.ErrSuspended) {
		t.Errorf("Any.Result err = %v; want sdk.ErrSuspended", err)
	}
	awaiting := wctx.snapshotAwaiting()
	if len(awaiting) != 2 {
		t.Errorf("awaiting = %v; want 2 tokens", awaiting)
	}
}

var _ frameStream = (*fakeStream)(nil)
