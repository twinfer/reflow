package eventsource

import (
	"errors"
	"fmt"
	"log/slog"

	wmnats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
)

// Required settings (backend.settings):
//   url              NATS server URL (nats://...)
//   durable_prefix   JetStream durable name prefix (forms a durable consumer)
// Optional:
//   queue_group_prefix   queue-group prefix for load-balanced consumers

func init() {
	RegisterFactory("nats", newNATSFactory())
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
