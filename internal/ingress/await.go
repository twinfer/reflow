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
// Completed status or the timeout fires. SSE/streaming is Phase 5; Phase 2
// uses server-side polling and bounds the wait at awaitMaxTimeout so a
// stalled handler can't hold the gRPC stream open indefinitely.
func (s *Server) AwaitInvocation(ctx context.Context, req *ingressv1.AwaitInvocationRequest) (*ingressv1.AwaitInvocationResponse, error) {
	id, err := resolveID(req.GetInvocationId(), req.GetInvocationIdProto())
	if err != nil {
		return nil, err
	}

	timeout := time.Duration(req.GetTimeoutMs()) * time.Millisecond
	if timeout <= 0 || timeout > awaitMaxTimeout {
		timeout = awaitMaxTimeout
	}
	deadline := time.Now().Add(timeout)
	shardID := id.GetPartitionKey()
	if shardID == 0 {
		shardID = Phase2ShardID
	}

	for {
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		st, err := s.host.LookupInvocationStatus(readCtx, shardID, id)
		cancel()
		if err == nil && st != nil {
			if c, ok := st.GetStatus().(*enginev1.InvocationStatus_Completed); ok {
				return &ingressv1.AwaitInvocationResponse{
					Output:         c.Completed.GetOutput(),
					FailureMessage: c.Completed.GetFailureMessage(),
					Completed:      true,
				}, nil
			}
		} else if err != nil && !isTransientLookupErr(err) {
			return nil, status.Errorf(codes.Internal, "lookup invocation: %v", err)
		}
		if time.Now().After(deadline) {
			return &ingressv1.AwaitInvocationResponse{Completed: false}, nil
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-time.After(awaitPollInterval):
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
	// Conservative: leave the rest to higher-level retry. Phase 4
	// multi-node will tighten this when we have leadership transitions
	// to ride through.
	return false
}
