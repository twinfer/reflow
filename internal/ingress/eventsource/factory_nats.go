package eventsource

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	wmnats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
)

// Required settings (backend.settings):
//   url              NATS server URL (nats://...)
//   durable_prefix   JetStream durable name prefix (forms a durable consumer)
// Optional:
//   queue_group_prefix   queue-group prefix for load-balanced consumers
//
// Naming constraint (enforced by natsValidator below): both `topic`
// and `durable_prefix` must be valid JetStream stream / consumer names
// (alphanumerics + `-` + `_`). NATS subjects allow dots as hierarchy
// separators, but watermill-nats reuses the topic verbatim as the
// stream name when AutoProvision creates the stream, and JetStream
// rejects dots in stream + consumer names. Operators who want
// dotted-subject routing should compose the subject in their publisher
// and use a dotless topic on the reflow side.

func init() {
	RegisterFactory("nats", newNATSFactory(), natsValidator)
}

// natsValidator rejects topic + durable_prefix values JetStream would
// refuse at stream / consumer creation time. The character set comes
// from JetStream's AddStream validation in nats-server; we mirror it
// here so operators see CodeInvalidArgument synchronously at upsert
// instead of a per-node "invalid stream name" reconcile failure.
func natsValidator(topic string, backend BackendConfig) error {
	if err := validateNATSName("topic", topic); err != nil {
		return err
	}
	if dp := backend.Settings["durable_prefix"]; dp != "" {
		if err := validateNATSName("backend.settings.durable_prefix", dp); err != nil {
			return err
		}
	}
	return nil
}

// validateNATSName checks one identifier against JetStream's
// stream/consumer name rule. The forbidden set is `. * > $` plus any
// whitespace; the empty case is reported by the caller's other checks.
func validateNATSName(label, name string) error {
	if name == "" {
		return nil
	}
	const forbidden = ".*>$ \t\n\r"
	if i := strings.IndexAny(name, forbidden); i >= 0 {
		return fmt.Errorf("nats: %s %q contains illegal char %q "+
			"(JetStream stream/consumer names allow alphanumerics + - + _)",
			label, name, name[i:i+1])
	}
	return nil
}

func newNATSFactory() Factory {
	return func(name string, backend BackendConfig, log *slog.Logger) (message.Subscriber, message.Publisher, error) {
		url := backend.Settings["url"]
		if url == "" {
			return nil, nil, errors.New("nats: backend.settings.url is required")
		}
		durablePrefix := backend.Settings["durable_prefix"]
		if durablePrefix == "" {
			return nil, nil, errors.New("nats: backend.settings.durable_prefix is required (JetStream-only)")
		}

		wmlog := watermillLogger(log)
		js := wmnats.JetStreamConfig{
			AutoProvision: true,
			DurablePrefix: durablePrefix,
			TrackMsgId:    true,
		}
		sub, err := wmnats.NewSubscriber(wmnats.SubscriberConfig{
			URL:              url,
			QueueGroupPrefix: backend.Settings["queue_group_prefix"],
			NatsOptions:      []nats.Option{nats.Name("reflow-eventsource-" + name)},
			JetStream:        js,
		}, wmlog)
		if err != nil {
			return nil, nil, fmt.Errorf("nats: subscriber for %q: %w", name, err)
		}

		pub, err := wmnats.NewPublisher(wmnats.PublisherConfig{
			URL:         url,
			NatsOptions: []nats.Option{nats.Name("reflow-eventsource-dlq-" + name)},
			JetStream:   js,
		}, wmlog)
		if err != nil {
			_ = sub.Close()
			return nil, nil, fmt.Errorf("nats: publisher for %q: %w", name, err)
		}
		return sub, pub, nil
	}
}
