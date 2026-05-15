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
// Signature matches google.golang.org/grpc/encoding.Codec so a customer
// can reuse a single Codec across the reflow handlerclient and standard
// gRPC stacks. Customer-supplied codecs (JSON, MessagePack, etc.)
// replace the default protobuf encoding without touching transport code.
//
// Both sides of a session must agree on the codec. For 5d the engine and
// the handler-side server are configured separately; pkg/sdk/server (5e)
// will take the matching option, and the HTTP Content-Type carries the
// codec name (application/vnd.reflow.invocation.v1+<codec>) for raw
// HTTP/2 negotiation.
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
