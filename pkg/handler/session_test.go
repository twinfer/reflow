package handler

import (
	"io"
	"testing"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// frameSourceFromSlice replays a fixed slice of frames in order, then
// returns io.EOF. Used by readReplay tests so we can hand-craft an
// engine-shaped frame stream without spinning up a real session.
type frameSourceFromSlice struct {
	frames []*protocolv1.Frame
	pos    int
}

func (s *frameSourceFromSlice) Recv() (*protocolv1.Frame, error) {
	if s.pos >= len(s.frames) {
		return nil, io.EOF
	}
	f := s.frames[s.pos]
	s.pos++
	return f, nil
}

// TestReadReplay_PlacesBySlot asserts the SDK respects the
// engine-stamped slot on each replay frame without decoding payloads.
// Mirrors the layout the engine builds for an invocation that did
// Input → Sleep(cmd+result) → SetState.
func TestReadReplay_PlacesBySlot(t *testing.T) {
	codec := handlerclient.DefaultCodec()
	inputPayload, err := codec.Marshal(&protocolv1.InputCommandMessage{
		Value: &protocolv1.Value{Content: []byte("hi")},
	})
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	// We don't decode these payloads in the test — readReplay should
	// place them at the stamped slot without touching the bytes.
	// Use arbitrary distinguishable opaque payloads.
	sleepCmdPayload := []byte("sleep-cmd-opaque")
	sleepNotePayload := []byte("sleep-note-opaque")
	setStatePayload := []byte("set-state-opaque")

	frames := []*protocolv1.Frame{
		handlerclient.FrameForSlot(handlerclient.TypeCmdInput, 0, inputPayload),
		handlerclient.FrameForSlot(handlerclient.TypeCmdSleep, 1, sleepCmdPayload),
		handlerclient.FrameForSlot(handlerclient.TypeNoteSleepDone, 2, sleepNotePayload),
		handlerclient.FrameForSlot(handlerclient.TypeCmdSetState, 3, setStatePayload),
	}
	src := &frameSourceFromSlice{frames: frames}

	input, replay, err := readReplay(src, codec, uint32(len(frames)))
	if err != nil {
		t.Fatalf("readReplay: %v", err)
	}
	if string(input) != "hi" {
		t.Errorf("input = %q; want %q", input, "hi")
	}
	if got := len(replay); got != 4 {
		t.Fatalf("replay len = %d; want 4", got)
	}

	// Each slot must hold the exact bytes the engine stamped — no
	// re-marshal, no decode-and-re-encode round trip.
	cases := []struct {
		slot    uint32
		typ     uint16
		payload []byte
	}{
		{0, handlerclient.TypeCmdInput, inputPayload},
		{1, handlerclient.TypeCmdSleep, sleepCmdPayload},
		{2, handlerclient.TypeNoteSleepDone, sleepNotePayload},
		{3, handlerclient.TypeCmdSetState, setStatePayload},
	}
	for _, c := range cases {
		e, ok := replay[c.slot]
		if !ok {
			t.Errorf("slot %d missing from replay", c.slot)
			continue
		}
		if e.typeCode != c.typ {
			t.Errorf("slot %d typeCode = 0x%04x; want 0x%04x", c.slot, e.typeCode, c.typ)
		}
		if string(e.payload) != string(c.payload) {
			t.Errorf("slot %d payload = %q; want %q (lazy decode must preserve bytes)",
				c.slot, e.payload, c.payload)
		}
	}
}

// TestReadReplay_NoDecodeOnOpaquePayloads asserts non-Input frames are
// never proto.Unmarshal'd during readReplay — placing only by stamped
// slot means even malformed payloads can pass through (they'd error
// later if the handler actually consults that slot).
func TestReadReplay_NoDecodeOnOpaquePayloads(t *testing.T) {
	codec := handlerclient.DefaultCodec()
	inputPayload, _ := codec.Marshal(&protocolv1.InputCommandMessage{
		Value: &protocolv1.Value{Content: nil},
	})
	garbage := []byte{0xff, 0xff, 0xff, 0xff} // not a valid protobuf

	frames := []*protocolv1.Frame{
		handlerclient.FrameForSlot(handlerclient.TypeCmdInput, 0, inputPayload),
		handlerclient.FrameForSlot(handlerclient.TypeNoteSleepDone, 1, garbage),
		handlerclient.FrameForSlot(handlerclient.TypeNoteCallDone, 2, garbage),
		handlerclient.FrameForSlot(handlerclient.TypeNoteRunDone, 3, garbage),
	}
	src := &frameSourceFromSlice{frames: frames}

	if _, _, err := readReplay(src, codec, uint32(len(frames))); err != nil {
		t.Errorf("readReplay rejected opaque payloads: %v; want nil (lazy decode defers to consumers)", err)
	}
}
