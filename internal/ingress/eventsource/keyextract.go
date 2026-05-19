package eventsource

import (
	"errors"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/tidwall/gjson"
)

// KeyExtractor pulls a string out of a Watermill message. Implementations
// are configured via EventSourceExtractor.From + .Value.
type KeyExtractor interface {
	Apply(msg *message.Message) (string, error)
}

// ErrExtractorMissing is returned when From is set but the message does
// not carry the named header / json path. It is a terminal classification
// (PoisonQueue diverts it to DLQ); a transient broker hiccup never
// surfaces here.
var ErrExtractorMissing = errors.New("eventsource: extractor key not found")

// newExtractor constructs an extractor from config. From="" returns nil
// (caller decides the default — uuid for idempotency, empty for object_key).
func newExtractor(from, value string) (KeyExtractor, error) {
	switch from {
	case "":
		return nil, nil
	case "uuid":
		return uuidExtractor{}, nil
	case "const":
		return constExtractor{value: value}, nil
	case "header":
		if value == "" {
			return nil, fmt.Errorf("eventsource: header extractor requires a value (header name)")
		}
		return headerExtractor{name: value}, nil
	case "json":
		if value == "" {
			return nil, fmt.Errorf("eventsource: json extractor requires a value (gjson path)")
		}
		return jsonExtractor{path: value}, nil
	default:
		return nil, fmt.Errorf("eventsource: unknown extractor type %q", from)
	}
}

type uuidExtractor struct{}

func (uuidExtractor) Apply(msg *message.Message) (string, error) {
	if msg.UUID == "" {
		return "", ErrExtractorMissing
	}
	return msg.UUID, nil
}

type constExtractor struct{ value string }

func (e constExtractor) Apply(_ *message.Message) (string, error) { return e.value, nil }

type headerExtractor struct{ name string }

func (e headerExtractor) Apply(msg *message.Message) (string, error) {
	v := msg.Metadata.Get(e.name)
	if v == "" {
		return "", fmt.Errorf("%w: header %q", ErrExtractorMissing, e.name)
	}
	return v, nil
}

type jsonExtractor struct{ path string }

func (e jsonExtractor) Apply(msg *message.Message) (string, error) {
	r := gjson.GetBytes(msg.Payload, e.path)
	if !r.Exists() {
		return "", fmt.Errorf("%w: json path %q", ErrExtractorMissing, e.path)
	}
	return r.String(), nil
}
