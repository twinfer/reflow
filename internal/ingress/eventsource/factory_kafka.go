package eventsource

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	wmkafka "github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Required settings (backend.settings):
//   brokers          comma-separated host:port list
//   consumer_group   sarama consumer group id
// Optional:
//   marshaler_partition_key   metadata key whose value is used as the kafka partition key on DLQ publish

func init() {
	RegisterFactory("kafka", newKafkaFactory(), kafkaValidator)
}

// kafkaValidator enforces Kafka's topic-name rules at upsert time.
// Kafka rejects topics longer than 249 chars or containing chars
// outside [A-Za-z0-9._-]; the broker would surface that as a runtime
// error, but doing the check at the admin RPC means operators see
// CodeInvalidArgument before the record is committed.
func kafkaValidator(topic string, _ BackendConfig) error {
	if len(topic) > 249 {
		return fmt.Errorf("kafka: topic %q is %d chars; max 249", topic, len(topic))
	}
	for i, r := range topic {
		switch {
		case r >= 'A' && r <= 'Z':
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '.' || r == '_' || r == '-':
		default:
			return fmt.Errorf("kafka: topic %q contains illegal char %q at index %d "+
				"(allowed: A-Z a-z 0-9 . _ -)", topic, string(r), i)
		}
	}
	return nil
}

func newKafkaFactory() Factory {
	return func(name string, backend BackendConfig, log *slog.Logger) (message.Subscriber, message.Publisher, error) {
		brokersCSV := backend.Settings["brokers"]
		if brokersCSV == "" {
			return nil, nil, errors.New("kafka: backend.settings.brokers is required")
		}
		brokers := splitCSV(brokersCSV)

		group := backend.Settings["consumer_group"]
		if group == "" {
			return nil, nil, errors.New("kafka: backend.settings.consumer_group is required")
		}

		wmlog := watermillLogger(log)
		sub, err := wmkafka.NewSubscriber(wmkafka.SubscriberConfig{
			Brokers:       brokers,
			ConsumerGroup: group,
			Unmarshaler:   wmkafka.DefaultMarshaler{},
		}, wmlog)
		if err != nil {
			return nil, nil, fmt.Errorf("kafka: subscriber for %q: %w", name, err)
		}

		pub, err := wmkafka.NewPublisher(wmkafka.PublisherConfig{
			Brokers:   brokers,
			Marshaler: wmkafka.DefaultMarshaler{},
		}, wmlog)
		if err != nil {
			_ = sub.Close()
			return nil, nil, fmt.Errorf("kafka: publisher for %q: %w", name, err)
		}
		return sub, pub, nil
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func watermillLogger(log *slog.Logger) watermill.LoggerAdapter {
	if log == nil {
		return watermill.NopLogger{}
	}
	return slogWatermillAdapter{log: log}
}
