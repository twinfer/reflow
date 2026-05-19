package eventsource

import (
	"errors"
	"time"

	"connectrpc.com/connect"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	backoffv5 "github.com/cenkalti/backoff/v5"
)

// terminalErr marks an error the dispatcher should not retry. Wrapped by
// the core handler before returning; both Retry's ShouldRetry and
// PoisonQueueWithFilter inspect it via errors.As. Once a terminalErr
// surfaces, the message is Acked (DLQ if configured) or dropped.
type terminalErr struct{ err error }

func (t *terminalErr) Error() string { return t.err.Error() }
func (t *terminalErr) Unwrap() error { return t.err }

// markTerminal wraps an error as terminal. Returns the original err if
// it already is.
func markTerminal(err error) error {
	if err == nil {
		return nil
	}
	var t *terminalErr
	if errors.As(err, &t) {
		return err
	}
	return &terminalErr{err: err}
}

// isTerminal reports whether the error chain contains a terminalErr.
func isTerminal(err error) bool {
	var t *terminalErr
	return errors.As(err, &t)
}

// classifyConnectErr decides whether a SubmitInvocation error is
// retry-eligible. Connect codes Unavailable / DeadlineExceeded / Unknown
// are transient (broker should retry); everything else (InvalidArgument,
// FailedPrecondition, PermissionDenied, ...) is terminal.
func classifyConnectErr(err error) error {
	if err == nil {
		return nil
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		switch ce.Code() {
		case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeUnknown, connect.CodeResourceExhausted:
			return err
		default:
			return markTerminal(err)
		}
	}
	return err
}

// compose builds the per-source middleware chain. Order (outermost first):
//
//	CorrelationID → PoisonQueue → Retry → core
//
// CorrelationID propagates a trace identifier through msg.Metadata.
// PoisonQueue (when DLQ topic configured) diverts terminal errors to a
// separate topic and Acks the original. Retry handles transient errors
// with exponential backoff; ShouldRetry returns false on terminal
// errors so they short-circuit straight to PoisonQueue.
func compose(core message.HandlerFunc, cfg SourceConfig, dlqPub message.Publisher, log watermill.LoggerAdapter) (message.HandlerFunc, error) {
	h := core
	h = retryMiddleware(cfg.Retry, log).Middleware(h)
	if cfg.DLQ.Topic != "" {
		if dlqPub == nil {
			// Should be unreachable: manager rejects DLQ-without-publisher.
			return nil, errors.New("eventsource: DLQ topic configured but no publisher available")
		}
		poison, err := middleware.PoisonQueueWithFilter(dlqPub, cfg.DLQ.Topic, isTerminal)
		if err != nil {
			return nil, err
		}
		h = poison(h)
	}
	h = middleware.CorrelationID(h)
	return h, nil
}

func retryMiddleware(cfg RetryConfig, log watermill.LoggerAdapter) middleware.Retry {
	r := middleware.Retry{
		MaxRetries:      cfg.MaxRetries,
		InitialInterval: cfg.InitialInterval,
		MaxInterval:     cfg.MaxInterval,
		Multiplier:      cfg.Multiplier,
		Logger:          log,
		ShouldRetry: func(p middleware.RetryParams) bool {
			if isTerminal(p.Err) {
				return false
			}
			var perm *backoffv5.PermanentError
			return !errors.As(p.Err, &perm)
		},
	}
	if r.MaxRetries == 0 {
		r.MaxRetries = 3
	}
	if r.InitialInterval == 0 {
		r.InitialInterval = 100 * time.Millisecond
	}
	if r.MaxInterval == 0 {
		r.MaxInterval = 5 * time.Second
	}
	if r.Multiplier == 0 {
		r.Multiplier = 2.0
	}
	return r
}
