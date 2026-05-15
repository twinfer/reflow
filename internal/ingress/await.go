package ingress

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

const (
	awaitMaxTimeout   = 60 * time.Second
	awaitPollInterval = 50 * time.Millisecond
)

// AwaitInvocation polls SyncRead until the named invocation reaches the
// Completed status or the timeout fires. Uses server-side polling and bounds
// the wait at awaitMaxTimeout so a stalled handler can't hold the gRPC
// stream open indefinitely.
func (s *Server) AwaitInvocation(ctx context.Context, req *ingressv1.AwaitInvocationRequest) (*ingressv1.AwaitInvocationResponse, error) {
	id, err := resolveID(req.GetInvocationId(), req.GetInvocationIdProto())
	if err != nil {
		return nil, err
	}
	shardID, err := s.shardForID(id)
	if err != nil {
		return nil, err
	}
	c, err := s.pollUntilCompleted(ctx, id, shardID, req.GetTimeoutMs())
	if err != nil {
		return nil, err
	}
	if c == nil {
		return &ingressv1.AwaitInvocationResponse{Completed: false}, nil
	}
	return &ingressv1.AwaitInvocationResponse{
		Output:         c.GetOutput(),
		FailureMessage: c.GetFailureMessage(),
		Completed:      true,
	}, nil
}

// pollUntilCompleted long-polls LookupInvocationStatus until the
// invocation reaches Completed or the timeout fires. Returns the
// terminal Completed payload on success, nil on timeout, or a gRPC
// status error on transport failure / context cancellation. timeoutMs
// is clamped to (0, awaitMaxTimeout]; 0 maps to awaitMaxTimeout.
func (s *Server) pollUntilCompleted(ctx context.Context, id *enginev1.InvocationId, shardID uint64, timeoutMs uint32) (*enginev1.Completed, error) {
	timeout := time.Duration(timeoutMs) * time.Millisecond
	if timeout <= 0 || timeout > awaitMaxTimeout {
		timeout = awaitMaxTimeout
	}
	deadline := time.Now().Add(timeout)

	// One Ticker reused across iterations — time.After in a select-loop
	// allocates a fresh runtime.Timer on every poll that runs to
	// completion before it can be GC'd.
	ticker := time.NewTicker(awaitPollInterval)
	defer ticker.Stop()

	for {
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		st, err := s.host.LookupInvocationStatus(readCtx, shardID, id)
		cancel()
		if err == nil && st != nil {
			if c, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed); ok {
				return c.Completed, nil
			}
		} else if err != nil && !isTransientLookupErr(err) {
			return nil, status.Errorf(codes.Internal, "lookup invocation: %v", err)
		}
		if time.Now().After(deadline) {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-ticker.C:
		}
	}
}

// isTransientLookupErr classifies dragonboat read errors so the await loop
// keeps retrying through transient leadership gaps rather than returning
// Internal. The set of transient cases here mirrors what proposer.go
// classifies as IsTempError, plus context.DeadlineExceeded (the 1s
// per-poll cap above).
func isTransientLookupErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// Conservative: leave the rest to higher-level retry. This can be
	// tightened once leadership-transition error types are stable.
	return false
}
