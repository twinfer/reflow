package eventsource

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	connect "connectrpc.com/connect"
	"github.com/ThreeDotsLabs/watermill/message"

	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// Submitter is the in-process surface the dispatcher calls. Satisfied
// by *internal/ingress.Server.
type Submitter interface {
	SubmitInvocation(ctx context.Context, req *connect.Request[ingressv1.SubmitInvocationRequest]) (*connect.Response[ingressv1.SubmitInvocationResponse], error)
}

// Dispatcher is the per-source pump. One goroutine reads from sub,
// composes the middleware chain, and feeds SubmitInvocation. Ack/Nack
// is driven by handle's return value.
//
// inflight tracks pending submitter.SubmitInvocation calls so the
// Reconciler can drain gracefully when this source is removed or
// reconfigured. WaitGroup increments fire from the apply-path goroutine
// (the dispatcher's Run loop) before each SubmitInvocation call and
// decrement on return; callers wait via Manager.stopDispatcher with a
// bounded timeout (5s by default).
type Dispatcher struct {
	name        string
	topic       string
	service     string
	handler     string
	objectKey   KeyExtractor
	idempotency KeyExtractor
	sub         Source
	submitter   Submitter
	handle      message.HandlerFunc
	metrics     *Metrics
	log         *slog.Logger
	inflight    sync.WaitGroup
}

// Run subscribes and dispatches until ctx is cancelled or the
// subscriber's channel closes. Returns nil on graceful shutdown.
func (d *Dispatcher) Run(ctx context.Context) error {
	ch, err := d.sub.Subscribe(ctx, d.topic)
	if err != nil {
		return fmt.Errorf("eventsource: subscribe %q topic %q: %w", d.name, d.topic, err)
	}
	d.log.Info("eventsource: dispatcher started", "source", d.name, "topic", d.topic, "service", d.service, "handler", d.handler)
	for msg := range ch {
		if _, err := d.handle(msg); err != nil {
			msg.Nack()
			d.metrics.MessagesNacked.WithLabelValues(d.name).Inc()
			d.log.Warn("eventsource: message Nacked", "source", d.name, "uuid", msg.UUID, "err", err)
			continue
		}
		msg.Ack()
		d.metrics.MessagesAcked.WithLabelValues(d.name).Inc()
	}
	d.log.Info("eventsource: dispatcher stopped", "source", d.name)
	return nil
}

// core builds the inner HandlerFunc that translates one broker message
// into a SubmitInvocation call. Caller wraps it with retry/poison/correlation.
func (d *Dispatcher) core() message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		objKey, err := d.applyObjectKey(msg)
		if err != nil {
			return nil, markTerminal(err)
		}
		idem, err := d.applyIdempotency(msg)
		if err != nil {
			return nil, markTerminal(err)
		}
		req := connect.NewRequest(&ingressv1.SubmitInvocationRequest{
			Service:        d.service,
			Handler:        d.handler,
			ObjectKey:      objKey,
			Input:          msg.Payload,
			IdempotencyKey: idem,
		})
		// Track this call so Reconciler-driven removal can drain
		// in-flight work before tearing down the dispatcher. The
		// dispatcher's own Run goroutine is the only writer of these
		// increments; WaitGroup's standard "Add before Wait" discipline
		// is satisfied because Wait runs on a different goroutine in
		// Manager.stopDispatcher and only after the subscriber's
		// channel has been closed (no more Adds possible).
		d.inflight.Add(1)
		start := time.Now()
		_, err = d.submitter.SubmitInvocation(msg.Context(), req)
		dur := time.Since(start).Seconds() * 1000
		d.inflight.Done()
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		d.metrics.SubmitDurationMs.WithLabelValues(d.name, outcome).Observe(dur)
		return nil, classifyConnectErr(err)
	}
}

func (d *Dispatcher) applyObjectKey(msg *message.Message) (string, error) {
	if d.objectKey == nil {
		return "", nil
	}
	return d.objectKey.Apply(msg)
}

func (d *Dispatcher) applyIdempotency(msg *message.Message) (string, error) {
	if d.idempotency == nil {
		// Default: use msg.UUID as idempotency key. Empty UUID → no dedup.
		return msg.UUID, nil
	}
	return d.idempotency.Apply(msg)
}
