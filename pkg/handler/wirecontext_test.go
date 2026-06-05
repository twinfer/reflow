package handler

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/twinfer/reflw/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	protocolv1 "github.com/twinfer/reflw/proto/protocolv1"
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
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, wire.DefaultCodec(), cache, nil, 7, 0, "Svc", "Hdr", "", protocolv1.Kind_KIND_SERVICE)
	return wctx, stream
}

// newTestWireContextWithReplay seeds the replay buffer so tests can
// assert replay-hit branches of Sleep, SetState, etc.
func newTestWireContextWithReplay(t *testing.T, replay map[uint32]*replayEntry) (*wireContext, *fakeStream) {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, wire.DefaultCodec(), nil, replay, 7, 0, "Svc", "Hdr", "", protocolv1.Kind_KIND_SERVICE)
	return wctx, stream
}

// newTestWireContextWithBudget builds a wireContext with an explicit
// per-invocation step budget so step-budget tests can exhaust it
// without running thousands of operations.
func newTestWireContextWithBudget(t *testing.T, budget uint32) (*wireContext, *fakeStream) {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, wire.DefaultCodec(), nil, nil, 7, budget, "Svc", "Hdr", "", protocolv1.Kind_KIND_SERVICE)
	return wctx, stream
}

// newTestWireContextWithKind builds a wireContext stamped with a specific
// service/key/kind tuple — used by Promise tests to assert kind-based
// validation and workflow scoping.
func newTestWireContextWithKind(t *testing.T, service, key string, kind protocolv1.Kind) (*wireContext, *fakeStream) {
	t.Helper()
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, wire.DefaultCodec(), nil, nil, 7, 0, service, "run", key, kind)
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
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdSetState {
		t.Errorf("frame[0].type = 0x%04x; want 0x%04x", tc, wire.TypeCmdSetState)
	}
	var setMsg protocolv1.SetStateCommandMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &setMsg); err != nil {
		t.Fatalf("decode SetStateCommandMessage: %v", err)
	}
	if got := string(setMsg.GetKey()); got != "counter" {
		t.Errorf("set.key = %q; want %q", got, "counter")
	}
	if got := string(setMsg.GetValue().GetContent()); got != "42" {
		t.Errorf("set.value = %q; want %q", got, "42")
	}

	// ClearState frame.
	tc, _, _ = wire.UnpackHeader(stream.sent[1].GetHeader())
	if tc != wire.TypeCmdClearState {
		t.Errorf("frame[1].type = 0x%04x; want 0x%04x", tc, wire.TypeCmdClearState)
	}
	var clrMsg protocolv1.ClearStateCommandMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[1].GetPayload(), &clrMsg); err != nil {
		t.Fatalf("decode ClearStateCommandMessage: %v", err)
	}
	if got := string(clrMsg.GetKey()); got != "temp" {
		t.Errorf("clear.key = %q; want %q", got, "temp")
	}

	// ClearAllState frame.
	tc, _, _ = wire.UnpackHeader(stream.sent[2].GetHeader())
	if tc != wire.TypeCmdClearAllState {
		t.Errorf("frame[2].type = 0x%04x; want 0x%04x", tc, wire.TypeCmdClearAllState)
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

// TestWireContext_GetState_PartialMissEmitsLazyFetch verifies that when
// partialState=true and the key isn't in the eager cache, GetState emits
// a GetLazyStateCommandMessage and suspends pending the result.
func TestWireContext_GetState_PartialMissEmitsLazyFetch(t *testing.T) {
	wctx, stream := newTestWireContext(t, map[string][]byte{"present": []byte("v")})
	wctx.partialState = true

	if v, ok, err := wctx.GetState("present"); err != nil || !ok || string(v) != "v" {
		t.Errorf("present-key GetState = (%q, %v, %v); want (v, true, nil)", v, ok, err)
	}
	if len(stream.sent) != 0 {
		t.Errorf("present-key GetState emitted %d frame(s); want 0 (cache hit)", len(stream.sent))
	}

	v, ok, err := wctx.GetState("missing")
	if !errors.Is(err, ErrSuspended) {
		t.Errorf("partial-miss GetState err = %v; want ErrSuspended", err)
	}
	if ok || v != nil {
		t.Errorf("partial-miss GetState = (%q, %v); want (nil, false)", v, ok)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("partial-miss GetState emitted %d frame(s); want 1", len(stream.sent))
	}
	typeCode, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if typeCode != wire.TypeCmdGetLazyState {
		t.Errorf("emitted frame type = 0x%04x; want 0x%04x (TypeCmdGetLazyState)", typeCode, wire.TypeCmdGetLazyState)
	}
	var cmd protocolv1.GetLazyStateCommandMessage
	if err := wctx.codec.Unmarshal(stream.sent[0].GetPayload(), &cmd); err != nil {
		t.Fatalf("decode GetLazyStateCommandMessage: %v", err)
	}
	if string(cmd.GetKey()) != "missing" {
		t.Errorf("cmd.Key = %q; want %q", cmd.GetKey(), "missing")
	}
	// cmd at slot 1 (first allocation), result at slot 2.
	if cmd.GetResultCompletionId() != 2 {
		t.Errorf("cmd.ResultCompletionId = %d; want 2", cmd.GetResultCompletionId())
	}
}

// TestWireContext_GetState_LazyReplayHit verifies that when the replay
// buffer carries a JEGetStateResult at the result slot, GetState
// decodes it without emitting a fetch.
func TestWireContext_GetState_LazyReplayHit(t *testing.T) {
	codec := wire.DefaultCodec()
	// Result slot will be 2 (cmdSlot=1, resultSlot=2).
	note := &protocolv1.GetLazyStateCompletionNotificationMessage{
		CompletionId: 2,
		Result: &protocolv1.GetLazyStateCompletionNotificationMessage_Value{
			Value: &protocolv1.Value{Content: []byte("hello")},
		},
	}
	notePayload, err := codec.Marshal(note)
	if err != nil {
		t.Fatalf("marshal note: %v", err)
	}
	replay := map[uint32]*replayEntry{
		1: {typeCode: wire.TypeCmdGetLazyState, payload: nil},
		2: {typeCode: wire.TypeNoteGetLazyState, payload: notePayload},
	}
	wctx, stream := newTestWireContextWithReplay(t, replay)
	wctx.partialState = true

	v, ok, err := wctx.GetState("k")
	if err != nil {
		t.Fatalf("GetState err = %v; want nil", err)
	}
	if !ok || string(v) != "hello" {
		t.Errorf("GetState = (%q, %v); want (hello, true)", v, ok)
	}
	if len(stream.sent) != 0 {
		t.Errorf("replay-hit emitted %d frame(s); want 0", len(stream.sent))
	}

	// Repeat read should hit cache, not re-fetch.
	v2, ok2, err2 := wctx.GetState("k")
	if err2 != nil || !ok2 || string(v2) != "hello" {
		t.Errorf("repeat GetState = (%q, %v, %v); want (hello, true, nil)", v2, ok2, err2)
	}
}

// TestWireContext_GetState_LazyReplayVoid verifies an absent-key replay
// (JEGetStateResult{present=false}) returns (nil, false, nil) and
// caches the "absent" verdict so a repeat read doesn't re-fetch.
func TestWireContext_GetState_LazyReplayVoid(t *testing.T) {
	codec := wire.DefaultCodec()
	note := &protocolv1.GetLazyStateCompletionNotificationMessage{
		CompletionId: 2,
		Result: &protocolv1.GetLazyStateCompletionNotificationMessage_Void{
			Void: &protocolv1.Void{},
		},
	}
	notePayload, err := codec.Marshal(note)
	if err != nil {
		t.Fatalf("marshal note: %v", err)
	}
	replay := map[uint32]*replayEntry{
		1: {typeCode: wire.TypeCmdGetLazyState, payload: nil},
		2: {typeCode: wire.TypeNoteGetLazyState, payload: notePayload},
	}
	wctx, stream := newTestWireContextWithReplay(t, replay)
	wctx.partialState = true

	v, ok, err := wctx.GetState("missing")
	if err != nil {
		t.Fatalf("GetState err = %v; want nil", err)
	}
	if ok || v != nil {
		t.Errorf("GetState = (%q, %v); want (nil, false)", v, ok)
	}
	// Second call: absent verdict cached → no fresh slot allocation, no
	// emit. The wireContext should return absent inline.
	v2, ok2, err2 := wctx.GetState("missing")
	if err2 != nil || ok2 || v2 != nil {
		t.Errorf("repeat GetState = (%q, %v, %v); want (nil, false, nil)", v2, ok2, err2)
	}
	if len(stream.sent) != 0 {
		t.Errorf("absent-replay emitted %d frame(s); want 0", len(stream.sent))
	}
}

// TestWireContext_GetState_ClearStateShortCircuits verifies a
// session-local ClearState marks the key absent so a subsequent GetState
// doesn't lazy-fetch even when partialState=true.
func TestWireContext_GetState_ClearStateShortCircuits(t *testing.T) {
	wctx, stream := newTestWireContext(t, map[string][]byte{"k": []byte("v")})
	wctx.partialState = true

	if err := wctx.ClearState("k"); err != nil {
		t.Fatalf("ClearState: %v", err)
	}
	v, ok, err := wctx.GetState("k")
	if err != nil || ok || v != nil {
		t.Errorf("GetState after ClearState = (%q, %v, %v); want (nil, false, nil)", v, ok, err)
	}
	// 1 frame: the ClearState. No lazy fetch.
	if len(stream.sent) != 1 {
		t.Errorf("emitted %d frame(s); want 1 (ClearState only)", len(stream.sent))
	}
	typeCode, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if typeCode != wire.TypeCmdClearState {
		t.Errorf("emitted frame type = 0x%04x; want TypeCmdClearState", typeCode)
	}
}

// TestWireContext_ClearState_AbsentKeysBoundByPartialState verifies the
// memory shape of absentKeys. The map is only useful when partialState=true
// (it short-circuits a 2-slot lazy fetch on a key we just cleared); when
// partialState=false the cache-miss + complete-snapshot path returns
// absent without consulting it, so writing growth-unbounded entries is
// pure waste. The test asserts ClearState honors the bound:
//
//   - partialState=true  → absentKeys[k] gets set
//   - partialState=false → absentKeys stays nil
//
// Both paths must still produce the right GetState answer (absent) for
// the cleared key.
func TestWireContext_ClearState_AbsentKeysBoundByPartialState(t *testing.T) {
	t.Run("partial=true writes absentKeys", func(t *testing.T) {
		wctx, _ := newTestWireContext(t, map[string][]byte{"k": []byte("v")})
		wctx.partialState = true
		if err := wctx.ClearState("k"); err != nil {
			t.Fatalf("ClearState: %v", err)
		}
		if !wctx.absentKeys["k"] {
			t.Errorf("absentKeys[k] = false; want true (partialState=true)")
		}
		v, ok, err := wctx.GetState("k")
		if err != nil || ok || v != nil {
			t.Errorf("GetState after ClearState = (%q, %v, %v); want (nil, false, nil)", v, ok, err)
		}
	})
	t.Run("partial=false skips absentKeys", func(t *testing.T) {
		wctx, _ := newTestWireContext(t, map[string][]byte{"k": []byte("v")})
		// partialState defaults false.
		if err := wctx.ClearState("k"); err != nil {
			t.Fatalf("ClearState: %v", err)
		}
		if len(wctx.absentKeys) != 0 {
			t.Errorf("absentKeys = %v; want empty (partialState=false makes it redundant)", wctx.absentKeys)
		}
		v, ok, err := wctx.GetState("k")
		if err != nil || ok || v != nil {
			t.Errorf("GetState after ClearState = (%q, %v, %v); want (nil, false, nil)", v, ok, err)
		}
	})
}

// TestWireContext_GetState_ClearAllStateClearsPartialFlag verifies
// ClearAllState flips partialState=false so subsequent reads return
// absent directly without lazy fetching.
func TestWireContext_GetState_ClearAllStateClearsPartialFlag(t *testing.T) {
	wctx, stream := newTestWireContext(t, map[string][]byte{"k": []byte("v")})
	wctx.partialState = true

	if err := wctx.ClearAllState(); err != nil {
		t.Fatalf("ClearAllState: %v", err)
	}
	if wctx.partialState {
		t.Errorf("partialState = true after ClearAllState; want false")
	}
	v, ok, err := wctx.GetState("k")
	if err != nil || ok || v != nil {
		t.Errorf("GetState after ClearAllState = (%q, %v, %v); want (nil, false, nil)", v, ok, err)
	}
	if len(stream.sent) != 1 {
		t.Errorf("emitted %d frame(s); want 1 (ClearAllState only)", len(stream.sent))
	}
}

// TestWireContext_GetStateKeys_EagerFreshEmits verifies that with a
// complete eager snapshot (partialState=false) GetStateKeys takes the
// 1-slot eager path: derives the sorted key list locally and emits
// GetEagerStateKeysCommandMessage with the keys inline. No suspension.
func TestWireContext_GetStateKeys_EagerFreshEmits(t *testing.T) {
	// Intentionally unsorted to verify snapshotEagerKeys sorts.
	wctx, stream := newTestWireContext(t, map[string][]byte{"c": []byte("3"), "a": []byte("1"), "b": []byte("2")})

	keys, err := wctx.GetStateKeys()
	if err != nil {
		t.Fatalf("GetStateKeys err = %v; want nil", err)
	}
	if got, want := strings.Join(keys, ","), "a,b,c"; got != want {
		t.Errorf("GetStateKeys = %v; want %q (sorted)", keys, want)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("emitted %d frame(s); want 1", len(stream.sent))
	}
	typeCode, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if typeCode != wire.TypeCmdGetEagerStateKeys {
		t.Errorf("emitted frame type = 0x%04x; want TypeCmdGetEagerStateKeys", typeCode)
	}
	var cmd protocolv1.GetEagerStateKeysCommandMessage
	if err := wctx.codec.Unmarshal(stream.sent[0].GetPayload(), &cmd); err != nil {
		t.Fatalf("decode GetEagerStateKeysCommandMessage: %v", err)
	}
	gotKeys := cmd.GetValue().GetKeys()
	if len(gotKeys) != 3 || string(gotKeys[0]) != "a" || string(gotKeys[1]) != "b" || string(gotKeys[2]) != "c" {
		t.Errorf("frame keys = %v; want [a b c]", gotKeys)
	}
	// Eager is 1-slot — nextSlot must have advanced by exactly 1.
	if wctx.nextSlot != 2 {
		t.Errorf("nextSlot = %d after eager GetStateKeys; want 2", wctx.nextSlot)
	}
}

// TestWireContext_GetStateKeys_EagerReplayHit verifies a JEGetEagerStateKeys
// replay entry decodes into the returned key list without re-emitting.
// Also covers the "snapshot changed between sessions" case: stateCache is
// empty on the replaying session but the journal-frozen keys are returned.
func TestWireContext_GetStateKeys_EagerReplayHit(t *testing.T) {
	codec := wire.DefaultCodec()
	cmd := &protocolv1.GetEagerStateKeysCommandMessage{
		Value: &protocolv1.StateKeys{
			Keys: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		},
	}
	cmdPayload, err := codec.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal cmd: %v", err)
	}
	replay := map[uint32]*replayEntry{
		1: {typeCode: wire.TypeCmdGetEagerStateKeys, payload: cmdPayload},
	}
	wctx, stream := newTestWireContextWithReplay(t, replay)

	keys, err := wctx.GetStateKeys()
	if err != nil {
		t.Fatalf("GetStateKeys err = %v; want nil", err)
	}
	if got, want := strings.Join(keys, ","), "a,b,c"; got != want {
		t.Errorf("GetStateKeys = %v; want %q", keys, want)
	}
	if len(stream.sent) != 0 {
		t.Errorf("replay-hit emitted %d frame(s); want 0", len(stream.sent))
	}
	// Single slot consumed.
	if wctx.nextSlot != 2 {
		t.Errorf("nextSlot = %d; want 2", wctx.nextSlot)
	}
}

// TestWireContext_GetStateKeys_LazyFreshEmits verifies the partial-snapshot
// path: GetStateKeys allocates 2 slots, emits GetLazyStateKeysCommandMessage,
// and suspends pending the result.
func TestWireContext_GetStateKeys_LazyFreshEmits(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)
	wctx.partialState = true

	keys, err := wctx.GetStateKeys()
	if !errors.Is(err, ErrSuspended) {
		t.Errorf("GetStateKeys err = %v; want ErrSuspended", err)
	}
	if keys != nil {
		t.Errorf("GetStateKeys = %v; want nil on suspend", keys)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("emitted %d frame(s); want 1", len(stream.sent))
	}
	typeCode, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if typeCode != wire.TypeCmdGetLazyStateKeys {
		t.Errorf("emitted frame type = 0x%04x; want TypeCmdGetLazyStateKeys", typeCode)
	}
	var cmd protocolv1.GetLazyStateKeysCommandMessage
	if err := wctx.codec.Unmarshal(stream.sent[0].GetPayload(), &cmd); err != nil {
		t.Fatalf("decode GetLazyStateKeysCommandMessage: %v", err)
	}
	if cmd.GetResultCompletionId() != 2 {
		t.Errorf("cmd.ResultCompletionId = %d; want 2", cmd.GetResultCompletionId())
	}
	if wctx.nextSlot != 3 {
		t.Errorf("nextSlot = %d after lazy GetStateKeys; want 3", wctx.nextSlot)
	}
}

// TestWireContext_GetStateKeys_LazyReplayHit verifies the partial-snapshot
// replay path: with TypeCmdGetLazyStateKeys at slot N, the SDK consumes
// both N and N+1 (the result notification) and returns its keys.
func TestWireContext_GetStateKeys_LazyReplayHit(t *testing.T) {
	codec := wire.DefaultCodec()
	note := &protocolv1.GetLazyStateKeysCompletionNotificationMessage{
		CompletionId: 2,
		StateKeys: &protocolv1.StateKeys{
			Keys: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		},
	}
	notePayload, err := codec.Marshal(note)
	if err != nil {
		t.Fatalf("marshal note: %v", err)
	}
	replay := map[uint32]*replayEntry{
		1: {typeCode: wire.TypeCmdGetLazyStateKeys, payload: nil},
		2: {typeCode: wire.TypeNoteGetLazyStateKeys, payload: notePayload},
	}
	wctx, stream := newTestWireContextWithReplay(t, replay)
	// partialState doesn't matter on replay — the slot-frame type decides.

	keys, err := wctx.GetStateKeys()
	if err != nil {
		t.Fatalf("GetStateKeys err = %v; want nil", err)
	}
	if got, want := strings.Join(keys, ","), "a,b,c"; got != want {
		t.Errorf("GetStateKeys = %v; want %q", keys, want)
	}
	if len(stream.sent) != 0 {
		t.Errorf("replay-hit emitted %d frame(s); want 0", len(stream.sent))
	}
	if wctx.nextSlot != 3 {
		t.Errorf("nextSlot = %d; want 3 (2 slots consumed)", wctx.nextSlot)
	}
}

// TestWireContext_GetStateKeys_EagerHonorsSessionWrites verifies the
// eager keys list reflects session-local writes — SetState adds, ClearState
// removes.
func TestWireContext_GetStateKeys_EagerHonorsSessionWrites(t *testing.T) {
	wctx, _ := newTestWireContext(t, map[string][]byte{"a": []byte("1"), "b": []byte("2")})
	if err := wctx.SetState("c", []byte("3")); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	if err := wctx.ClearState("a"); err != nil {
		t.Fatalf("ClearState: %v", err)
	}

	keys, err := wctx.GetStateKeys()
	if err != nil {
		t.Fatalf("GetStateKeys err = %v; want nil", err)
	}
	if got, want := strings.Join(keys, ","), "b,c"; got != want {
		t.Errorf("GetStateKeys = %v; want %q", keys, want)
	}
}

// TestWireContext_SlotAllocation verifies allocSlot advances by span and
// matches inproc's contract: slot 0 is reserved for JEInput; user calls
// start at slot 1.
func TestWireContext_SlotAllocation(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)

	got, err := wctx.allocSlot(1)
	if err != nil || got != 1 {
		t.Errorf("first allocSlot(1) = (%d, %v); want (1, nil)", got, err)
	}
	got, err = wctx.allocSlot(2)
	if err != nil || got != 2 {
		t.Errorf("second allocSlot(2) = (%d, %v); want (2, nil)", got, err)
	}
	got, err = wctx.allocSlot(1)
	if err != nil || got != 4 {
		t.Errorf("third allocSlot(1) = (%d, %v); want (4, nil)", got, err)
	}
}

// TestWireContext_Sleep_FreshEmits asserts the first call to Sleep on
// a session with empty replay emits SleepCommandMessage and suspends.
func TestWireContext_Sleep_FreshEmits(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	fut := wctx.Sleep(50 * time.Millisecond)
	_, err := fut.Result()
	if !errors.Is(err, ErrSuspended) {
		t.Errorf("Sleep.Result err = %v; want ErrSuspended", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1 (SleepCommandMessage)", len(stream.sent))
	}
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdSleep {
		t.Errorf("frame.type = 0x%04x; want 0x%04x", tc, wire.TypeCmdSleep)
	}
	var msg protocolv1.SleepCommandMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &msg); err != nil {
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
	codec := wire.DefaultCodec()
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
		1: {typeCode: wire.TypeCmdSleep, payload: cmdPayload},
		2: {typeCode: wire.TypeNoteSleepDone, payload: notePayload},
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
		1: {typeCode: wire.TypeCmdSetState, payload: nil},
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

	fut := wctx.Call(Target{Service: "Echo", Handler: "echo", Key: "k1"}, []byte("hi"))
	_, err := fut.Result()
	if !errors.Is(err, ErrSuspended) {
		t.Errorf("Call.Result err = %v; want ErrSuspended", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1 (CallCommandMessage)", len(stream.sent))
	}
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdCall {
		t.Errorf("frame.type = 0x%04x; want 0x%04x", tc, wire.TypeCmdCall)
	}
	var msg protocolv1.CallCommandMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &msg); err != nil {
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
	codec := wire.DefaultCodec()
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
		1: {typeCode: wire.TypeCmdCall},
		2: {typeCode: wire.TypeNoteCallDone, payload: notePayload},
	})

	v, err := wctx.Call(Target{Service: "X", Handler: "y"}, nil).Result()
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
	codec := wire.DefaultCodec()
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
		1: {typeCode: wire.TypeCmdCall},
		2: {typeCode: wire.TypeNoteCallDone, payload: notePayload},
	})

	_, err = wctx.Call(Target{Service: "X", Handler: "y"}, nil).Result()
	if err == nil {
		t.Fatal("Call.Result err = nil; want non-nil failure")
	}
	f, ok := AsFailure(err)
	if !ok {
		t.Fatalf("Call.Result err = %v; want *Failure", err)
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
	if err := wctx.OneWayCall(Target{Service: "X", Handler: "y", Key: "k1"}, []byte("p")); err != nil {
		t.Fatalf("OneWayCall: %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1", len(stream.sent))
	}
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdOneWayCall {
		t.Errorf("frame.type = 0x%04x; want 0x%04x", tc, wire.TypeCmdOneWayCall)
	}
}

// TestWireContext_OneWayCall_ReplayHitSkipsEmit asserts a replay-hit
// OneWayCall is a no-op (engine already journaled it).
func TestWireContext_OneWayCall_ReplayHitSkipsEmit(t *testing.T) {
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: wire.TypeCmdOneWayCall},
	})
	if err := wctx.OneWayCall(Target{Service: "X", Handler: "y"}, nil); err != nil {
		t.Errorf("OneWayCall: %v", err)
	}
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (replay hit should not re-emit)", len(stream.sent))
	}
}

// TestWireContext_SendSignal_FreshEmits asserts SendSignal emits a
// SendSignalCommandMessage at slot 1 and returns nil — mirrors the
// OneWayCall single-slot pattern.
func TestWireContext_SendSignal_FreshEmits(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)
	err := wctx.SendSignal(Target{Service: "Counter", Handler: "Increment", Key: "alice"}, "tap", []byte("p"))
	if err != nil {
		t.Fatalf("SendSignal: %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1", len(stream.sent))
	}
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdSendSignal {
		t.Errorf("frame.type = 0x%04x; want 0x%04x (TypeCmdSendSignal)", tc, wire.TypeCmdSendSignal)
	}
}

// TestWireContext_SendSignal_ReplayHitSkipsEmit asserts replay sees the
// pre-journaled entry and skips re-emit.
func TestWireContext_SendSignal_ReplayHitSkipsEmit(t *testing.T) {
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: wire.TypeCmdSendSignal},
	})
	err := wctx.SendSignal(Target{Service: "Counter", Handler: "h", Key: "alice"}, "tap", nil)
	if err != nil {
		t.Errorf("SendSignal: %v", err)
	}
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (replay hit should not re-emit)", len(stream.sent))
	}
}

// TestWireContext_CancelInvocation_EmitsCancelSignal asserts
// CancelInvocation desugars to a __cancel__ named SendSignal.
func TestWireContext_CancelInvocation_EmitsCancelSignal(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)
	target := Target{Service: "Counter", Handler: "Increment", Key: "alice"}
	if err := wctx.CancelInvocation(target); err != nil {
		t.Fatalf("CancelInvocation: %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1", len(stream.sent))
	}
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdSendSignal {
		t.Errorf("frame.type = 0x%04x; want 0x%04x (TypeCmdSendSignal)", tc, wire.TypeCmdSendSignal)
	}
	// Decode and verify signal_name is WellKnownCancelSignal.
	var cmd protocolv1.SendSignalCommandMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &cmd); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cmd.GetSignalName() != WellKnownCancelSignal {
		t.Errorf("signal_name = %q; want %q", cmd.GetSignalName(), WellKnownCancelSignal)
	}
	if cmd.GetKey() != "alice" {
		t.Errorf("key = %q; want alice", cmd.GetKey())
	}
}

// TestWireContext_CancelInvocation_RejectsUnkeyedTarget asserts cancel
// inherits SendSignal's unkeyed-target guard.
func TestWireContext_CancelInvocation_RejectsUnkeyedTarget(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)
	err := wctx.CancelInvocation(Target{Service: "svc", Handler: "h"})
	if err == nil {
		t.Fatalf("CancelInvocation with empty Key: expected error, got nil")
	}
	f, ok := AsFailure(err)
	if !ok || f.Code != SendSignalUnkeyedCode {
		t.Errorf("err = %v; want *Failure(SendSignalUnkeyedCode)", err)
	}
}

// TestWireContext_WaitSignal_FreshEmitsAndSuspends asserts WaitSignal
// emits AwaitSignalCommandMessage on a fresh run and the returned
// future suspends with a signal:<name>:<slot> token.
func TestWireContext_WaitSignal_FreshEmitsAndSuspends(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)
	fut := wctx.WaitSignal("ready")
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1", len(stream.sent))
	}
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdAwaitSignal {
		t.Errorf("frame.type = 0x%04x; want 0x%04x (TypeCmdAwaitSignal)", tc, wire.TypeCmdAwaitSignal)
	}
	// Decode and verify cmd_slot / result_slot are 1 / 2.
	var cmd protocolv1.AwaitSignalCommandMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &cmd); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cmd.GetSignalName() != "ready" {
		t.Errorf("signal_name = %q; want ready", cmd.GetSignalName())
	}
	if cmd.GetResultCompletionId() != 2 {
		t.Errorf("result_completion_id = %d; want 2", cmd.GetResultCompletionId())
	}

	// First Poll: not yet resolved; token is signal:ready:2.
	poller := fut.(Poller)
	resolved, tokens := poller.Poll()
	if resolved {
		t.Error("Poll returned resolved=true for a fresh WaitSignal")
	}
	if len(tokens) != 1 || tokens[0] != "signal:ready:2" {
		t.Errorf("Poll tokens = %v; want [signal:ready:2]", tokens)
	}

	// Result() should suspend.
	_, err := fut.Result()
	if err != ErrSuspended {
		t.Errorf("Result err = %v; want ErrSuspended", err)
	}
}

// TestWireContext_WaitSignal_ReplayResultReturnsPayload asserts that
// when the resultSlot replay entry carries a SignalNotificationMessage
// (delivered by the engine on a prior run), WaitSignal's future
// resolves to the payload without re-emitting the command.
func TestWireContext_WaitSignal_ReplayResultReturnsPayload(t *testing.T) {
	// Replay carries:
	//   slot 1: TypeCmdAwaitSignal (the prior command)
	//   slot 2: TypeNoteSignal carrying the resolved payload
	notePayload, err := wire.DefaultCodec().Marshal(&protocolv1.SignalNotificationMessage{
		SignalId: &protocolv1.SignalNotificationMessage_Name{Name: "ready"},
		Result: &protocolv1.SignalNotificationMessage_Value{
			Value: &protocolv1.Value{Content: []byte("payload-1")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: wire.TypeCmdAwaitSignal},
		2: {typeCode: wire.TypeNoteSignal, payload: notePayload},
	})
	fut := wctx.WaitSignal("ready")
	// Replay-hit on cmdSlot: nothing is sent.
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (replay hit should not re-emit)", len(stream.sent))
	}
	got, err := fut.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if string(got) != "payload-1" {
		t.Errorf("payload = %q; want payload-1", got)
	}
}

// TestWireContext_WaitSignal_AllocatesTwoSlots asserts WaitSignal
// reserves cmdSlot + resultSlot (= cmdSlot + 1) like Awakeable, so a
// subsequent primitive lands at slot 3.
func TestWireContext_WaitSignal_AllocatesTwoSlots(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)
	wctx.WaitSignal("ready")
	if err := wctx.SetState("k", []byte("v")); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	// Frames: 1 = TypeCmdAwaitSignal, 2 = TypeCmdSetState. SetState
	// landed at slot 3 (after cmdSlot + resultSlot).
	if len(stream.sent) != 2 {
		t.Fatalf("sent = %d; want 2", len(stream.sent))
	}
	if wctx.nextSlot != 4 {
		t.Errorf("nextSlot = %d; want 4 (1 cmd + 1 result + 1 SetState = next 4)", wctx.nextSlot)
	}
}

// TestWireContext_Run_FreshExecutesAndEmitsBothFrames asserts the
// happy path: fn() runs locally, RunCommandMessage +
// ProposeRunCompletionMessage are emitted in order, and the inline
// return path surfaces fn's value.
func TestWireContext_Run_FreshExecutesAndEmitsBothFrames(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	ranCount := 0
	v, err := wctx.Run("compute", func(*RunContext) ([]byte, error) {
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
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdRun {
		t.Errorf("frame[0].type = 0x%04x; want 0x%04x (TypeCmdRun)", tc, wire.TypeCmdRun)
	}
	tc, _, _ = wire.UnpackHeader(stream.sent[1].GetHeader())
	if tc != wire.TypeProposeRunDone {
		t.Errorf("frame[1].type = 0x%04x; want 0x%04x (TypeProposeRunDone)", tc, wire.TypeProposeRunDone)
	}
	var prop protocolv1.ProposeRunCompletionMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[1].GetPayload(), &prop); err != nil {
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

	_, err := wctx.Run("fetch", func(*RunContext) ([]byte, error) {
		return nil, errors.New("network blip")
	})
	if !errors.Is(err, ErrSuspended) {
		t.Errorf("Run err = %v; want ErrSuspended (retryable)", err)
	}
	var prop protocolv1.ProposeRunCompletionMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[1].GetPayload(), &prop); err != nil {
		t.Fatalf("decode ProposeRunCompletionMessage: %v", err)
	}
	if !prop.GetRetryable() {
		t.Error("prop.retryable = false; want true for transient error")
	}
}

// TestWireContext_Run_FailureIsTerminal asserts a returned *Failure
// is recorded as terminal (retryable=false) and surfaced from Run.
func TestWireContext_Run_FailureIsTerminal(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	_, err := wctx.Run("validate", func(*RunContext) ([]byte, error) {
		return nil, NewFailure(0, "bad input")
	})
	f, ok := AsFailure(err)
	if !ok {
		t.Fatalf("Run err = %v; want *Failure", err)
	}
	if f.Message != "bad input" {
		t.Errorf("failure.message = %q; want %q", f.Message, "bad input")
	}
	var prop protocolv1.ProposeRunCompletionMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[1].GetPayload(), &prop); err != nil {
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
	codec := wire.DefaultCodec()
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
		1: {typeCode: wire.TypeNoteRunDone, payload: notePayload},
	})

	ranCount := 0
	v, err := wctx.Run("compute", func(*RunContext) ([]byte, error) {
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

// TestWireContext_StepBudget_Exhausts asserts ctx.SetState (and any
// primitive that calls allocSlot) surfaces a *Failure with
// StepBudgetExhaustedCode once the per-invocation cap is reached. The
// session loop catches the *Failure and completes the invocation
// terminally.
func TestWireContext_StepBudget_Exhausts(t *testing.T) {
	// budget=3 means slots 1 and 2 are free; the third primitive call
	// (which would allocate slot 3 == budget) must fail.
	wctx, _ := newTestWireContextWithBudget(t, 3)

	if err := wctx.SetState("a", []byte("1")); err != nil {
		t.Fatalf("first SetState err = %v; want nil (within budget)", err)
	}
	if err := wctx.SetState("b", []byte("2")); err != nil {
		t.Fatalf("second SetState err = %v; want nil (within budget)", err)
	}

	err := wctx.SetState("c", []byte("3"))
	f, ok := AsFailure(err)
	if !ok {
		t.Fatalf("third SetState err = %v; want *Failure", err)
	}
	if f.Code != StepBudgetExhaustedCode {
		t.Errorf("failure.code = %d; want %d (StepBudgetExhaustedCode)",
			f.Code, StepBudgetExhaustedCode)
	}
	if !strings.Contains(f.Message, "step budget") {
		t.Errorf("failure.message = %q; want 'step budget' substring", f.Message)
	}
}

// TestWireContext_StepBudget_MultiSlotAllocs asserts a multi-slot
// primitive (Call, Awakeable, Sleep — all 2-slot) is rejected when
// the remaining budget can't fit both slots, even if 1 slot remains.
func TestWireContext_StepBudget_MultiSlotAllocs(t *testing.T) {
	// budget=3 — slots 1, 2 free. A 2-slot allocation now (would use
	// 1, 2) succeeds; a follow-up 2-slot allocation needs slots 3, 4
	// which both lie at/beyond the cap → exhausted.
	wctx, _ := newTestWireContextWithBudget(t, 3)

	if err := wctx.SetState("a", []byte("1")); err != nil {
		t.Fatalf("first SetState err = %v; want nil", err)
	}
	// Now nextSlot=2, budget=3. allocSlot(2) wants slots 2, 3 →
	// 2+2 > 3 → exhausted.
	fut := wctx.Sleep(time.Millisecond)
	_, err := fut.Result()
	f, ok := AsFailure(err)
	if !ok {
		t.Fatalf("Sleep result err = %v; want *Failure", err)
	}
	if f.Code != StepBudgetExhaustedCode {
		t.Errorf("failure.code = %d; want %d", f.Code, StepBudgetExhaustedCode)
	}
}

// TestWireContext_StepBudget_ZeroBudgetSkipsCheck asserts budget=0
// disables the SDK pre-flight entirely (the wire-session backstop
// still enforces). This is the path tests and the embedded harness
// take when no DeploymentRecord was resolved.
func TestWireContext_StepBudget_ZeroBudgetSkipsCheck(t *testing.T) {
	wctx, _ := newTestWireContextWithBudget(t, 0)
	for i := range 50 {
		if err := wctx.SetState("k", []byte("v")); err != nil {
			t.Fatalf("SetState #%d err = %v; want nil (budget=0 disables check)", i, err)
		}
	}
}

// TestWireContext_Run_RunContextExposed asserts the first attempt
// receives a RunContext with attempt=1 and a stable idempotency key
// stamped onto both the marker frame and the user-visible context.
func TestWireContext_Run_RunContextExposed(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	var seenAttempt uint32
	var seenKey string
	if _, err := wctx.Run("compute", func(rc *RunContext) ([]byte, error) {
		seenAttempt = rc.Attempt()
		seenKey = rc.IdempotencyKey()
		return []byte("ok"), nil
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if seenAttempt != 1 {
		t.Errorf("RunContext.Attempt() = %d; want 1 on first call", seenAttempt)
	}
	if seenKey == "" {
		t.Fatal("RunContext.IdempotencyKey() empty; want stamped value")
	}

	var marker protocolv1.RunCommandMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &marker); err != nil {
		t.Fatalf("decode RunCommandMessage: %v", err)
	}
	if marker.GetAttempt() != 1 {
		t.Errorf("marker.attempt = %d; want 1", marker.GetAttempt())
	}
	if marker.GetIdempotencyKey() != seenKey {
		t.Errorf("marker.idempotency_key = %q; want %q (same as RunContext)",
			marker.GetIdempotencyKey(), seenKey)
	}
}

// TestWireContext_Run_RetryReplayBumpsAttempt asserts that when the
// engine respawns after a retryable failure, the replayed marker's
// attempt counter is surfaced to fn — so the user sees a fresh
// attempt + idempotency key on the second invocation of fn.
func TestWireContext_Run_RetryReplayBumpsAttempt(t *testing.T) {
	codec := wire.DefaultCodec()
	markerPayload, err := codec.Marshal(&protocolv1.RunCommandMessage{
		ResultCompletionId: 1,
		Attempt:            2,
		IdempotencyKey:     "engine-stamped-key",
	})
	if err != nil {
		t.Fatalf("marshal marker: %v", err)
	}
	wctx, _ := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: wire.TypeCmdRun, payload: markerPayload},
	})

	var seenAttempt uint32
	var seenKey string
	_, err = wctx.Run("compute", func(rc *RunContext) ([]byte, error) {
		seenAttempt = rc.Attempt()
		seenKey = rc.IdempotencyKey()
		return []byte("retry-ok"), nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if seenAttempt != 2 {
		t.Errorf("RunContext.Attempt() = %d; want 2 on respawn", seenAttempt)
	}
	if seenKey != "engine-stamped-key" {
		t.Errorf("RunContext.IdempotencyKey() = %q; want engine-stamped value", seenKey)
	}
}

// TestWireContext_Run_OptionsThreadPolicyOntoWire asserts MaxAttempts
// + Backoff flow onto the ProposeRunCompletionMessage so the engine
// can honour per-call retry budgets.
func TestWireContext_Run_OptionsThreadPolicyOntoWire(t *testing.T) {
	wctx, stream := newTestWireContext(t, nil)

	_, _ = wctx.Run("fetch",
		func(*RunContext) ([]byte, error) { return nil, errors.New("blip") },
		MaxAttempts(5),
		Backoff(200*time.Millisecond, 3.0, 20*time.Second),
	)

	var prop protocolv1.ProposeRunCompletionMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[1].GetPayload(), &prop); err != nil {
		t.Fatalf("decode prop: %v", err)
	}
	p := prop.GetRetryPolicy()
	if p == nil {
		t.Fatal("retry_policy nil; want populated from RunOptions")
	}
	if p.GetMaxAttempts() != 5 {
		t.Errorf("policy.max_attempts = %d; want 5", p.GetMaxAttempts())
	}
	if p.GetInitialIntervalMs() != 200 {
		t.Errorf("policy.initial_interval_ms = %d; want 200", p.GetInitialIntervalMs())
	}
	if p.GetFactor() != 3.0 {
		t.Errorf("policy.factor = %v; want 3.0", p.GetFactor())
	}
	if p.GetMaxIntervalMs() != 20_000 {
		t.Errorf("policy.max_interval_ms = %d; want 20000", p.GetMaxIntervalMs())
	}
}

// TestWireContext_Run_IdempotencyKeyDifferentPerAttempt asserts the
// derived key changes between attempts (so downstream dedup can
// distinguish a retry from a duplicate).
func TestWireContext_Run_IdempotencyKeyDifferentPerAttempt(t *testing.T) {
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	k1 := deriveIdempotencyKey(id, 1, 1)
	k2 := deriveIdempotencyKey(id, 1, 2)
	if k1 == k2 {
		t.Errorf("idempotency key collapsed across attempts: %q", k1)
	}
	if len(k1) != 16 {
		t.Errorf("key len = %d; want 16 hex chars", len(k1))
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
	if _, err := fut.Result(); !errors.Is(err, ErrSuspended) {
		t.Errorf("Awakeable future.Result err = %v; want ErrSuspended", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1 (AwakeableCommandMessage)", len(stream.sent))
	}
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdAwakeable {
		t.Errorf("frame.type = 0x%04x; want 0x%04x", tc, wire.TypeCmdAwakeable)
	}
	var msg protocolv1.AwakeableCommandMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &msg); err != nil {
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
	codec := wire.DefaultCodec()
	cmdPayload, err := codec.Marshal(&protocolv1.AwakeableCommandMessage{
		ResultCompletionId: 2,
		AwakeableId:        "awk_replayid1234567890abcd",
	})
	if err != nil {
		t.Fatalf("marshal AwakeableCommandMessage: %v", err)
	}
	signalPayload, err := codec.Marshal(&protocolv1.SignalNotificationMessage{
		SignalId: &protocolv1.SignalNotificationMessage_Name{
			Name: "awk_replayid1234567890abcd",
		},
		Result: &protocolv1.SignalNotificationMessage_Value{
			Value: &protocolv1.Value{Content: []byte("resolved")},
		},
	})
	if err != nil {
		t.Fatalf("marshal SignalNotificationMessage: %v", err)
	}
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: wire.TypeCmdAwakeable, payload: cmdPayload},
		2: {typeCode: wire.TypeNoteSignal, payload: signalPayload},
	})

	id, fut := wctx.Awakeable()
	if id != "awk_replayid1234567890abcd" {
		t.Errorf("Awakeable id = %q; want %q", id, "awk_replayid1234567890abcd")
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
	codec := wire.DefaultCodec()
	cmdPayload, err := codec.Marshal(&protocolv1.AwakeableCommandMessage{
		ResultCompletionId: 2,
		AwakeableId:        "awk_pending1234567890abcde",
	})
	if err != nil {
		t.Fatalf("marshal AwakeableCommandMessage: %v", err)
	}
	wctx, stream := newTestWireContextWithReplay(t, map[uint32]*replayEntry{
		1: {typeCode: wire.TypeCmdAwakeable, payload: cmdPayload},
	})

	id, fut := wctx.Awakeable()
	if id != "awk_pending1234567890abcde" {
		t.Errorf("Awakeable id = %q; want cached id", id)
	}
	if _, err := fut.Result(); !errors.Is(err, ErrSuspended) {
		t.Errorf("future.Result err = %v; want ErrSuspended (cmd-only replay)", err)
	}
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (cmd already journaled)", len(stream.sent))
	}
}

// TestWireContext_Awakeable_IDEmbedsPartitionKey asserts the minted id
// encodes partitionKey at bytes [0:8] — the contract
// ingress.ResolveAwakeable depends on for routing. Body layout is
// [8B owner partition_key][8B random].
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
// once suspended, every ctx call returns ErrSuspended.
func TestWireContext_Suspend_ShortCircuitsSubsequentCalls(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)

	// First Sleep call suspends.
	if _, err := wctx.Sleep(time.Second).Result(); !errors.Is(err, ErrSuspended) {
		t.Fatalf("first Sleep.Result err = %v; want ErrSuspended", err)
	}
	// SetState after suspend short-circuits without emitting.
	if err := wctx.SetState("k", []byte("v")); !errors.Is(err, ErrSuspended) {
		t.Errorf("SetState after suspend err = %v; want ErrSuspended", err)
	}
	// Second Sleep also short-circuits.
	if _, err := wctx.Sleep(time.Second).Result(); !errors.Is(err, ErrSuspended) {
		t.Errorf("second Sleep.Result err = %v; want ErrSuspended", err)
	}
}

// TestWireContext_SendSignal_RejectsUnkeyedTarget verifies that the SDK
// guards against signaling unkeyed services — there's no well-defined
// receiver since multiple concurrent invocations may share (service,
// handler) when no Key is supplied.
func TestWireContext_SendSignal_RejectsUnkeyedTarget(t *testing.T) {
	wctx, _ := newTestWireContext(t, nil)

	err := wctx.SendSignal(Target{Service: "svc", Handler: "h"}, "s", nil)
	if err == nil {
		t.Fatalf("SendSignal with empty Key: expected error, got nil")
	}
	f, ok := AsFailure(err)
	if !ok {
		t.Fatalf("SendSignal err = %v (%T); want *Failure", err, err)
	}
	if f.Code != SendSignalUnkeyedCode {
		t.Errorf("Failure.Code = %d; want %d", f.Code, SendSignalUnkeyedCode)
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
	if !errors.Is(err, ErrSuspended) {
		t.Errorf("All.Results err = %v; want ErrSuspended", err)
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
	if !errors.Is(err, ErrSuspended) {
		t.Errorf("Any.Result err = %v; want ErrSuspended", err)
	}
	awaiting := wctx.snapshotAwaiting()
	if len(awaiting) != 2 {
		t.Errorf("awaiting = %v; want 2 tokens", awaiting)
	}
}

// TestWireContext_Promise_RejectsNonWorkflowKind asserts Promise methods
// surface PromiseNotWorkflowFailure when called from a non-workflow
// handler, without polluting the journal.
func TestWireContext_Promise_RejectsNonWorkflowKind(t *testing.T) {
	wctx, stream := newTestWireContextWithKind(t, "Svc", "", protocolv1.Kind_KIND_SERVICE)
	p := wctx.Promise("done")

	if err := p.Resolve([]byte("v")); err == nil {
		t.Fatal("Resolve: expected failure for non-workflow kind")
	} else if f, ok := AsFailure(err); !ok || f.Code != PromiseNotWorkflowCode {
		t.Errorf("Resolve err = %v; want PromiseNotWorkflowFailure", err)
	}

	if _, _, _, err := p.Peek(); err != nil {
		// Peek returns failure as the third value, not err.
		t.Errorf("Peek unexpected err = %v", err)
	}
	_, _, fail, _ := p.Peek()
	if fail == nil || fail.Code != PromiseNotWorkflowCode {
		t.Errorf("Peek failure = %v; want PromiseNotWorkflowFailure", fail)
	}

	fut := p.Result()
	if _, err := fut.Result(); err == nil {
		t.Fatal("Result: expected failure for non-workflow kind")
	} else if f, ok := AsFailure(err); !ok || f.Code != PromiseNotWorkflowCode {
		t.Errorf("Result err = %v; want PromiseNotWorkflowFailure", err)
	}

	// No frames sent because each method rejected before allocSlot.
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (non-workflow methods must not emit)", len(stream.sent))
	}
	if wctx.nextSlot != 1 {
		t.Errorf("nextSlot = %d; want 1 (no slot allocation expected)", wctx.nextSlot)
	}
}

// TestWireContext_Promise_Result_FreshEmitsAndSuspends asserts
// Promise.Result emits TypeCmdGetPromise and the future suspends on
// the resultSlot token.
func TestWireContext_Promise_Result_FreshEmitsAndSuspends(t *testing.T) {
	wctx, stream := newTestWireContextWithKind(t, "Wf", "k-1", protocolv1.Kind_KIND_WORKFLOW)
	fut := wctx.Promise("done").Result()
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1", len(stream.sent))
	}
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdGetPromise {
		t.Errorf("frame.type = 0x%04x; want TypeCmdGetPromise", tc)
	}
	var cmd protocolv1.GetPromiseCommandMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &cmd); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cmd.GetName() != "done" || cmd.GetKey() != "k-1" || cmd.GetService() != "Wf" || cmd.GetResultCompletionId() != 2 {
		t.Errorf("cmd = %+v; want name=done service=Wf key=k-1 result_completion_id=2", &cmd)
	}

	if _, err := fut.Result(); err != ErrSuspended {
		t.Errorf("Result err = %v; want ErrSuspended", err)
	}
}

// TestWireContext_WorkflowPromise_UsesExplicitScope asserts that
// ctx.WorkflowPromise(target, name) populates the wire CompletePromise
// frame's Service and Key from target — NOT from the caller's own
// (service, key) which the apply path uses for cross-partition routing.
func TestWireContext_WorkflowPromise_UsesExplicitScope(t *testing.T) {
	wctx, stream := newTestWireContextWithKind(t, "Workers", "rs-key", protocolv1.Kind_KIND_WORKFLOW)
	fut := wctx.WorkflowPromise(Target{Service: "Orders", Key: "wf-key"}, "done").Result()
	if len(stream.sent) != 1 {
		t.Fatalf("sent frames = %d; want 1", len(stream.sent))
	}
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdGetPromise {
		t.Fatalf("frame.type = 0x%04x; want TypeCmdGetPromise", tc)
	}
	var cmd protocolv1.GetPromiseCommandMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &cmd); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cmd.GetService() != "Orders" || cmd.GetKey() != "wf-key" || cmd.GetName() != "done" {
		t.Errorf("cmd = %+v; want service=Orders key=wf-key name=done", &cmd)
	}
	if _, err := fut.Result(); err != ErrSuspended {
		t.Errorf("Result err = %v; want ErrSuspended", err)
	}
}

// TestWireContext_Promise_Result_ReplayHitReturnsValue asserts that
// when the resultSlot replay entry carries a
// GetPromiseCompletionNotificationMessage, Result returns the cached
// value without re-emitting.
func TestWireContext_Promise_Result_ReplayHitReturnsValue(t *testing.T) {
	notePayload, err := wire.DefaultCodec().Marshal(&protocolv1.GetPromiseCompletionNotificationMessage{
		CompletionId: 2,
		Result: &protocolv1.GetPromiseCompletionNotificationMessage_Value{
			Value: &protocolv1.Value{Content: []byte("done!")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, wire.DefaultCodec(),
		nil,
		map[uint32]*replayEntry{
			1: {typeCode: wire.TypeCmdGetPromise},
			2: {typeCode: wire.TypeNoteGetPromise, payload: notePayload},
		},
		7, 0, "Wf", "run", "k-1", protocolv1.Kind_KIND_WORKFLOW)

	fut := wctx.Promise("done").Result()
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (replay hit)", len(stream.sent))
	}
	got, err := fut.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if string(got) != "done!" {
		t.Errorf("payload = %q; want done!", got)
	}
}

// TestWireContext_Promise_Resolve_FreshEmitsAndSuspends asserts
// Promise.Resolve emits TypeCmdCompletePromise carrying the value and
// suspends waiting for the ack.
func TestWireContext_Promise_Resolve_FreshEmitsAndSuspends(t *testing.T) {
	wctx, stream := newTestWireContextWithKind(t, "Wf", "k-1", protocolv1.Kind_KIND_WORKFLOW_SHARED)
	err := wctx.Promise("done").Resolve([]byte("value-1"))
	if err != ErrSuspended {
		t.Errorf("Resolve err = %v; want ErrSuspended (waiting on ack)", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent = %d; want 1", len(stream.sent))
	}
	tc, _, _ := wire.UnpackHeader(stream.sent[0].GetHeader())
	if tc != wire.TypeCmdCompletePromise {
		t.Errorf("frame.type = 0x%04x; want TypeCmdCompletePromise", tc)
	}
	var cmd protocolv1.CompletePromiseCommandMessage
	if err := wire.DefaultCodec().Unmarshal(stream.sent[0].GetPayload(), &cmd); err != nil {
		t.Fatalf("decode: %v", err)
	}
	v := cmd.GetCompletionValue()
	if v == nil || string(v.GetContent()) != "value-1" {
		t.Errorf("completion value = %+v; want content=value-1", v)
	}
}

// TestWireContext_Promise_Resolve_ReplayAckReturnsSuccess asserts that
// when the ack slot replay entry is present (succeeded), Resolve
// returns nil without re-emitting.
func TestWireContext_Promise_Resolve_ReplayAckReturnsSuccess(t *testing.T) {
	ackPayload, err := wire.DefaultCodec().Marshal(&protocolv1.CompletePromiseCompletionNotificationMessage{
		CompletionId: 2,
		Result:       &protocolv1.CompletePromiseCompletionNotificationMessage_Void{Void: &protocolv1.Void{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, wire.DefaultCodec(),
		nil,
		map[uint32]*replayEntry{
			1: {typeCode: wire.TypeCmdCompletePromise},
			2: {typeCode: wire.TypeNoteCompletePromise, payload: ackPayload},
		},
		7, 0, "Wf", "run", "k-1", protocolv1.Kind_KIND_WORKFLOW)

	if err := wctx.Promise("done").Resolve([]byte("v")); err != nil {
		t.Errorf("Resolve err = %v; want nil (replay ack=succeeded)", err)
	}
	if len(stream.sent) != 0 {
		t.Errorf("sent %d frames; want 0 (replay hit)", len(stream.sent))
	}
}

// TestWireContext_Promise_Resolve_ReplayAckSurfacesAlreadyCompleted
// asserts that when the ack is a Failure notification (succeeded=false
// path), Resolve surfaces a *Failure.
func TestWireContext_Promise_Resolve_ReplayAckSurfacesAlreadyCompleted(t *testing.T) {
	ackPayload, err := wire.DefaultCodec().Marshal(&protocolv1.CompletePromiseCompletionNotificationMessage{
		CompletionId: 2,
		Result: &protocolv1.CompletePromiseCompletionNotificationMessage_Failure{
			Failure: &protocolv1.Failure{Code: PromiseAlreadyCompletedCode, Message: "promise already completed"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := &enginev1.InvocationId{PartitionKey: 7, Uuid: []byte("0123456789ABCDEF")}
	stream := &fakeStream{}
	wctx := newWireContext(t.Context(), id, []byte("hello"), stream, wire.DefaultCodec(),
		nil,
		map[uint32]*replayEntry{
			1: {typeCode: wire.TypeCmdCompletePromise},
			2: {typeCode: wire.TypeNoteCompletePromise, payload: ackPayload},
		},
		7, 0, "Wf", "run", "k-1", protocolv1.Kind_KIND_WORKFLOW)

	err = wctx.Promise("done").Resolve([]byte("v"))
	f, ok := AsFailure(err)
	if !ok {
		t.Fatalf("Resolve err = %v; want *Failure", err)
	}
	if f.Code != PromiseAlreadyCompletedCode {
		t.Errorf("Resolve failure code = %d; want PromiseAlreadyCompletedCode (%d)", f.Code, PromiseAlreadyCompletedCode)
	}
}

var (
	_ frameSource = (*fakeStream)(nil)
	_ frameSink   = (*fakeStream)(nil)
)
