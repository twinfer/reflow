package eventsource

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	wmsqs "github.com/ThreeDotsLabs/watermill-aws/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// Required settings (backend.settings):
//   region   AWS region
// Optional:
//   profile           AWS shared-config profile (overrides default)
//
// Queue URL is derived from the Watermill topic (which is the SQS queue
// name); the default GetQueueUrlByName resolver handles the lookup.
// Long-running invocation handlers risk re-delivery — v1 does not extend
// visibility timeout on the fly. Configure a generous queue-level
// VisibilityTimeout on the SQS side.

func init() {
	RegisterFactory("sqs", newSQSFactory(), sqsValidator)
}

// sqsValidator enforces AWS's SQS queue-name rules at upsert time.
// Standard queue names are ≤80 chars over [A-Za-z0-9_-]. FIFO queues
// are the same set + a literal `.fifo` suffix; we strip that suffix
// before validating the rest. AWS would reject a malformed name at
// runtime, but doing it here surfaces CodeInvalidArgument
// synchronously to the operator.
func sqsValidator(topic string, _ BackendConfig) error {
	name := topic
	const fifoSuffix = ".fifo"
	if len(name) > len(fifoSuffix) && name[len(name)-len(fifoSuffix):] == fifoSuffix {
		name = name[:len(name)-len(fifoSuffix)]
	}
	if len(topic) == 0 {
		// generic non-empty check catches this; defensive guard.
		return nil
	}
	if len(topic) > 80 {
		return fmt.Errorf("sqs: queue name %q is %d chars; max 80 (incl. .fifo suffix)", topic, len(topic))
	}
	for i, r := range name {
		switch {
		case r >= 'A' && r <= 'Z':
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '-':
		default:
			return fmt.Errorf("sqs: queue name %q contains illegal char %q at index %d "+
				"(allowed: A-Z a-z 0-9 _ -; .fifo suffix permitted)",
				topic, string(r), i)
		}
	}
	return nil
}

func newSQSFactory() Factory {
	return func(name string, backend BackendConfig, log *slog.Logger) (message.Subscriber, message.Publisher, error) {
		region := backend.Settings["region"]
		if region == "" {
			return nil, nil, errors.New("sqs: backend.settings.region is required")
		}
		loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
		if profile := backend.Settings["profile"]; profile != "" {
			loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(profile))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
		if err != nil {
			return nil, nil, fmt.Errorf("sqs: load aws config for %q: %w", name, err)
		}

		wmlog := watermillLogger(log)
		sub, err := wmsqs.NewSubscriber(wmsqs.SubscriberConfig{
			AWSConfig: awsCfg,
		}, wmlog)
		if err != nil {
			return nil, nil, fmt.Errorf("sqs: subscriber for %q: %w", name, err)
		}

		pub, err := wmsqs.NewPublisher(wmsqs.PublisherConfig{
			AWSConfig: awsCfg,
		}, wmlog)
		if err != nil {
			_ = sub.Close()
			return nil, nil, fmt.Errorf("sqs: publisher for %q: %w", name, err)
		}
		return sub, pub, nil
	}
}
