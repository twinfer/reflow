package sdk

import (
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// PayloadCodec encodes and decodes user domain values to/from the raw
// `[]byte` that the engine durably stores (handler input, handler output,
// Run results, Call inputs/outputs, awakeable payloads).
//
// The engine never inspects payload bytes — it just stores them. The
// codec lives entirely on the SDK side, mirroring Temporal's
// DataConverter and Restate's serde model. Choose one codec for a
// (service, handler) family so producers and consumers agree on the
// envelope; mixing codecs requires explicit out-of-band tagging.
//
// Implementations should be safe for concurrent use. Encode must never
// mutate value; Decode must accept any *T pointer the caller passes.
//
// Built-in codecs:
//
//   - JSONCodec — encoding/json with default settings (handles any
//     json-marshalable Go type).
//   - ProtoCodec — google.golang.org/protobuf/proto for proto.Message
//     types. Fastest and most compact for known-schema payloads.
//   - RawBytesCodec — pass-through; the value must be []byte (or a *[]byte
//     on Decode). Useful when the SDK is handling already-encoded data
//     produced upstream.
//
// To layer behaviors (encryption, compression), wrap a base codec —
// e.g. NewEncryptingCodec(JSONCodec{}, key). Each layer should append a
// content tag to a metadata sidecar; reflow has no per-entry metadata
// channel today, so composition is left to the user.
type PayloadCodec interface {
	// Name returns a short, stable identifier for the codec. Used for
	// debug logs and future per-payload metadata so a
	// reader can pick the right codec by tag. Format: "encoding/scheme",
	// e.g. "json/default", "proto/binary", "bytes/raw".
	Name() string

	// Encode marshals value into bytes. value is typically a struct,
	// proto.Message, or primitive. Encoders MUST NOT mutate value.
	Encode(value any) ([]byte, error)

	// Decode unmarshals data into *valuePtr. valuePtr must be a non-nil
	// pointer to the target type; codecs reject other shapes with a
	// descriptive error.
	Decode(data []byte, valuePtr any) error
}

// ErrNilCodecTarget is returned by Decode implementations when valuePtr
// is nil or not a pointer. Callers can errors.Is against this to detect
// programmer error vs. payload-shape errors.
var ErrNilCodecTarget = errors.New("sdk: codec target must be a non-nil pointer")

// JSONCodec encodes with encoding/json. Default codec when the SDK is
// initialized without an explicit choice.
type JSONCodec struct{}

// Name implements PayloadCodec.
func (JSONCodec) Name() string { return "json/default" }

// Encode implements PayloadCodec.
func (JSONCodec) Encode(value any) ([]byte, error) {
	return json.Marshal(value)
}

// Decode implements PayloadCodec.
func (JSONCodec) Decode(data []byte, valuePtr any) error {
	if valuePtr == nil {
		return ErrNilCodecTarget
	}
	return json.Unmarshal(data, valuePtr)
}

// ProtoCodec encodes proto.Message values to wire-format binary protobuf.
// Encode rejects non-proto values; Decode requires valuePtr to point at
// a proto.Message.
type ProtoCodec struct{}

// Name implements PayloadCodec.
func (ProtoCodec) Name() string { return "proto/binary" }

// Encode implements PayloadCodec.
func (ProtoCodec) Encode(value any) ([]byte, error) {
	m, ok := value.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("sdk: ProtoCodec.Encode: value of type %T is not a proto.Message", value)
	}
	return proto.Marshal(m)
}

// Decode implements PayloadCodec.
func (ProtoCodec) Decode(data []byte, valuePtr any) error {
	if valuePtr == nil {
		return ErrNilCodecTarget
	}
	m, ok := valuePtr.(proto.Message)
	if !ok {
		return fmt.Errorf("sdk: ProtoCodec.Decode: target of type %T is not a proto.Message", valuePtr)
	}
	return proto.Unmarshal(data, m)
}

// RawBytesCodec is the no-op codec: value must be []byte on Encode, and
// valuePtr must be *[]byte on Decode. Useful for binary blobs the SDK
// receives pre-encoded from another system.
type RawBytesCodec struct{}

// Name implements PayloadCodec.
func (RawBytesCodec) Name() string { return "bytes/raw" }

// Encode implements PayloadCodec.
func (RawBytesCodec) Encode(value any) ([]byte, error) {
	b, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("sdk: RawBytesCodec.Encode: value of type %T is not []byte", value)
	}
	// Defensive copy so the caller can't mutate journaled bytes by
	// reusing the input slice.
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

// Decode implements PayloadCodec.
func (RawBytesCodec) Decode(data []byte, valuePtr any) error {
	if valuePtr == nil {
		return ErrNilCodecTarget
	}
	dst, ok := valuePtr.(*[]byte)
	if !ok {
		return fmt.Errorf("sdk: RawBytesCodec.Decode: target of type %T is not *[]byte", valuePtr)
	}
	out := make([]byte, len(data))
	copy(out, data)
	*dst = out
	return nil
}

// DefaultCodec is the codec used when no explicit choice is configured.
// Returns JSONCodec — broadest applicability, no codegen, zero deps
// beyond stdlib. Tests and helpers refer to this as the conventional
// fallback.
func DefaultCodec() PayloadCodec { return JSONCodec{} }
