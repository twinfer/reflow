// Package eventsource wires broker-driven invocation triggers into the
// reflow ingress. Each configured source binds one Watermill Subscriber
// (Kafka / NATS / SQS / gochannel) to one (service, handler) target;
// inbound messages are translated to SubmitInvocation requests and
// dispatched in-process against the local engine.
package eventsource

import "github.com/ThreeDotsLabs/watermill/message"

// Source is the broker-side feed for one dispatcher. Operators can
// provide custom adapters by implementing message.Subscriber directly
// and registering a Factory.
type Source = message.Subscriber
