package handlerclient

import (
	"fmt"

	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// Type codes per proto/protocolv1/protocol.proto. Comments cite the
// type-code namespacing convention. Keep in sync with the proto file —
// the codes are part of the wire contract.
const (
	// Core lifecycle (0x0000-0x00FF).
	TypeStart          uint16 = 0x0000
	TypeSuspension     uint16 = 0x0001
	TypeError          uint16 = 0x0002
	TypeEnd            uint16 = 0x0003
	TypeCommandAck     uint16 = 0x0004
	TypeProposeRunDone uint16 = 0x0005

	// Commands (0x0400-0x04FF). Only the subset wired today is listed;
	// the rest land as the wire-session matures (awakeable).
	TypeCmdInput         uint16 = 0x0400
	TypeCmdOutput        uint16 = 0x0401
	TypeCmdSetState      uint16 = 0x0403
	TypeCmdClearState    uint16 = 0x0404
	TypeCmdClearAllState uint16 = 0x0405
	TypeCmdSleep         uint16 = 0x040C
	TypeCmdCall          uint16 = 0x040D
	TypeCmdOneWayCall    uint16 = 0x040E

	TypeCmdRun uint16 = 0x0411

	// Notifications (0x8000-0x80FF).
	TypeNoteSleepDone        uint16 = 0x800C
	TypeNoteCallDone         uint16 = 0x800D
	TypeNoteCallInvocationId uint16 = 0x800E
	TypeNoteRunDone          uint16 = 0x8011
)

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

// ValidatePayload returns an error when the Frame's declared payload
// length disagrees with the actual bytes. Cheap consistency check; a
// peer could lie, but a mismatch always indicates corruption or a buggy
// peer.
func ValidatePayload(f *protocolv1.Frame) error {
	_, _, length := UnpackHeader(f.GetHeader())
	if int(length) != len(f.GetPayload()) {
		return fmt.Errorf("handlerclient: frame header length %d != payload bytes %d", length, len(f.GetPayload()))
	}
	return nil
}
