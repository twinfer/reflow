package handlerclient

import (
	"fmt"

	"google.golang.org/protobuf/proto"
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
		return nil, fmt.Errorf("handlerclient: protoCodec.Marshal: %T is not a proto.Message", v)
	}
	return proto.Marshal(m)
}

func (protoCodec) Unmarshal(data []byte, v any) error {
	m, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("handlerclient: protoCodec.Unmarshal: %T is not a proto.Message", v)
	}
	return proto.Unmarshal(data, m)
}

func (protoCodec) Name() string { return "protobuf" }
