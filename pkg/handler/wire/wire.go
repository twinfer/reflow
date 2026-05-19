// Package wire is the shared protocol vocabulary spoken by both the
// reflow engine (internal/engine/handlerclient) and the handler SDK
// (pkg/handler). It contains the Codec interface, the Type* frame-code
// constants, the Frame helpers, and Route — anything that needs the
// same definition on both sides of an engine↔handler session.
package wire

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// Codec encodes the inner protocolv1 message payloads carried inside a
// Frame's payload field. The Frame envelope itself is always protobuf
// for wire compatibility across reflow nodes and languages; Codec
// controls only the inner message encoding (StartMessage,
// OutputCommandMessage, etc.).
//
// Both sides of a session must agree on the codec. The engine and the
// handler-side server are configured separately; the HTTP Content-Type
// carries the codec name (application/vnd.reflow.invocation.v1+<codec>)
// so the handler can verify negotiation succeeded.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	Name() string
}

// DefaultCodec is the protobuf codec. The framing layer always uses
// protobuf for the outer Frame envelope; Codec controls only the inner
// payload encoding.
func DefaultCodec() Codec { return protoCodec{} }

type protoCodec struct{}

func (protoCodec) Marshal(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("wire: protoCodec.Marshal: %T is not a proto.Message", v)
	}
	return proto.Marshal(m)
}

func (protoCodec) Unmarshal(data []byte, v any) error {
	m, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("wire: protoCodec.Unmarshal: %T is not a proto.Message", v)
	}
	return proto.Unmarshal(data, m)
}

func (protoCodec) Name() string { return "protobuf" }

// Route is the per-session destination metadata: the (service, handler)
// tuple the engine wants to invoke. Connect RPC has no per-handler URL
// addressing, so this tuple flows inside the StartMessage frame the
// engine writes first.
type Route struct {
	Service string
	Handler string
}

// Type codes per proto/protocolv1/protocol.proto. The codes are part
// of the wire contract; keep this list in sync with the proto file.
// Codes for messages not yet emitted by either side (CommandAck,
// CallInvocationId) are intentionally omitted — add them here when
// their handler lands rather than carrying dead constants.
const (
	// Core lifecycle (0x0000-0x00FF).
	TypeStart          uint16 = 0x0000
	TypeSuspension     uint16 = 0x0001
	TypeError          uint16 = 0x0002
	TypeEnd            uint16 = 0x0003
	TypeProposeRunDone uint16 = 0x0005

	// Commands (0x0400-0x04FF).
	TypeCmdInput            uint16 = 0x0400
	TypeCmdOutput           uint16 = 0x0401
	TypeCmdGetLazyState     uint16 = 0x0402
	TypeCmdSetState         uint16 = 0x0403
	TypeCmdClearState       uint16 = 0x0404
	TypeCmdClearAllState    uint16 = 0x0405
	TypeCmdGetLazyStateKeys uint16 = 0x0406
	TypeCmdGetPromise       uint16 = 0x0409
	TypeCmdPeekPromise      uint16 = 0x040A
	TypeCmdCompletePromise  uint16 = 0x040B
	TypeCmdSleep            uint16 = 0x040C
	TypeCmdCall             uint16 = 0x040D
	TypeCmdOneWayCall       uint16 = 0x040E
	TypeCmdRun              uint16 = 0x0411
	TypeCmdAwakeable        uint16 = 0x0414
	TypeCmdSendSignal       uint16 = 0x0415
	TypeCmdAwaitSignal      uint16 = 0x0416

	// Notifications (0x8000-0x80FF).
	TypeNoteGetLazyState     uint16 = 0x8002
	TypeNoteGetLazyStateKeys uint16 = 0x8006
	TypeNoteGetPromise       uint16 = 0x8009
	TypeNotePeekPromise      uint16 = 0x800A
	TypeNoteCompletePromise  uint16 = 0x800B
	TypeNoteSleepDone        uint16 = 0x800C
	TypeNoteCallDone         uint16 = 0x800D
	TypeNoteRunDone          uint16 = 0x8011

	// Out-of-band signal delivery (0xFBFF). The same code carries
	// awakeable resolutions and any future numbered signals.
	TypeNoteSignal uint16 = 0xFBFF
)

// WellKnownCancelSignal is the reserved signal name interpreted by the
// receiver shard as "force this invocation to terminate with
// CancelledCode". Sent via ctx.CancelInvocation or the ingress
// CancelInvocation RPC. The engine special-cases this name in the
// SignalDelivered apply arm, bypassing the normal signal inbox/awaiter
// path. Lives in pkg/handler/wire because it is shared engine↔handler
// vocabulary; both sides must agree on the literal string.
const WellKnownCancelSignal = "__cancel__"

// CancelledCode is the reserved Failure.Code stamped onto invocations
// terminated by a __cancel__ signal. Mirrored by handler.CancelledCode
// for the SDK side; defined here so internal/engine can reference it
// without importing pkg/handler.
const CancelledCode uint32 = 9002

// PackHeader encodes (type, flags, payload length) into the 64-bit
// big-endian header word stored on protocolv1.Frame.header.
//
//	[16-bit type | 16-bit flags | 32-bit length]
func PackHeader(typeCode, flags uint16, length uint32) uint64 {
	return uint64(typeCode)<<48 | uint64(flags)<<32 | uint64(length)
}

// UnpackHeader splits the 64-bit header word into its three fields.
func UnpackHeader(h uint64) (typeCode, flags uint16, length uint32) {
	typeCode = uint16(h >> 48)
	flags = uint16(h >> 32)
	length = uint32(h)
	return
}

// FrameFor wraps an already-encoded payload in a Frame. The length
// field on the header is computed from len(payload); callers should
// pass payload exactly as Codec.Marshal returned it.
func FrameFor(typeCode uint16, payload []byte) *protocolv1.Frame {
	return &protocolv1.Frame{
		Header:  PackHeader(typeCode, 0, uint32(len(payload))),
		Payload: payload,
	}
}

// FrameForSlot is FrameFor + a stamped slot. Used by the engine when
// shipping replay frames so the SDK can place each entry by slot
// without decoding the payload to extract completion_id / matching
// awakeable id.
func FrameForSlot(typeCode uint16, slot uint32, payload []byte) *protocolv1.Frame {
	return &protocolv1.Frame{
		Header:  PackHeader(typeCode, 0, uint32(len(payload))),
		Payload: payload,
		Slot:    slot,
	}
}

// ValidatePayload returns an error when the Frame's declared payload
// length disagrees with the actual bytes. Cheap consistency check; a
// peer could lie, but a mismatch always indicates corruption or a buggy
// peer.
func ValidatePayload(f *protocolv1.Frame) error {
	_, _, length := UnpackHeader(f.GetHeader())
	if int(length) != len(f.GetPayload()) {
		return fmt.Errorf("wire: frame header length %d != payload bytes %d", length, len(f.GetPayload()))
	}
	return nil
}
